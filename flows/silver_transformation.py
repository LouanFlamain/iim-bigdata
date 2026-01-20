from io import BytesIO
from datetime import datetime

import pandas as pd
from prefect import flow, task

from config import BUCKET_BRONZE, BUCKET_SILVER, get_minio_client


def log_event(event: str, **kwargs):
    timestamp = datetime.now().isoformat()
    details = " | ".join(f"{k}={v}" for k, v in kwargs.items())
    print(f"[{timestamp}] {event} | {details}")


@task(name="read_bronze_csv", retries=3, retry_delay_seconds=[2, 10, 30])
def read_bronze_csv(object_name: str) -> pd.DataFrame:
    client = get_minio_client()
    response = client.get_object(BUCKET_BRONZE, object_name)
    df = pd.read_csv(response)
    response.close()
    response.release_conn()
    log_event("READ_BRONZE", object=object_name, rows=len(df))
    return df


@task(name="clean_clients")
def clean_clients(df: pd.DataFrame) -> pd.DataFrame:
    rows_before = len(df)

    df = df.drop_duplicates(subset=["id_client"])
    df["date_inscription"] = pd.to_datetime(df["date_inscription"])
    df["pays"] = df["pays"].str.strip().str.title()
    df["email"] = df["email"].str.lower().str.strip()
    df = df.dropna(subset=["id_client", "email"])

    rows_after = len(df)
    log_event("CLEAN_CLIENTS", rows_before=rows_before, rows_after=rows_after, rows_dropped=rows_before - rows_after)
    return df


@task(name="clean_achats")
def clean_achats(df: pd.DataFrame, valid_client_ids: set) -> pd.DataFrame:
    rows_before = len(df)

    df = df.drop_duplicates(subset=["id_achat"])
    df["date_achat"] = pd.to_datetime(df["date_achat"])
    df["produit"] = df["produit"].str.strip().str.title()
    df = df[df["montant"] > 0]
    df = df.dropna(subset=["id_achat", "id_client", "montant"])

    rows_after_basic = len(df)

    df = df[df["id_client"].isin(valid_client_ids)]
    rows_after_ref = len(df)

    orphans = rows_after_basic - rows_after_ref
    log_event(
        "CLEAN_ACHATS",
        rows_before=rows_before,
        rows_after=rows_after_ref,
        rows_dropped_basic=rows_before - rows_after_basic,
        orphan_records=orphans
    )
    return df


@task(name="write_silver_parquet", retries=3, retry_delay_seconds=[2, 10, 30])
def write_silver_parquet(df: pd.DataFrame, object_name: str) -> str:
    client = get_minio_client()

    if not client.bucket_exists(BUCKET_SILVER):
        client.make_bucket(BUCKET_SILVER)

    buffer = BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)

    client.put_object(
        BUCKET_SILVER,
        object_name,
        buffer,
        length=buffer.getbuffer().nbytes,
        content_type="application/octet-stream"
    )
    log_event("WRITE_SILVER", object=object_name, rows=len(df))
    return object_name


@flow(name="Silver Transformation Flow")
def silver_transformation_flow() -> dict:
    log_event("FLOW_START", flow="silver_transformation")

    clients_df = read_bronze_csv("clients.csv")
    achats_df = read_bronze_csv("achats.csv")

    clients_clean = clean_clients(clients_df)
    valid_client_ids = set(clients_clean["id_client"].unique())

    achats_clean = clean_achats(achats_df, valid_client_ids)

    silver_clients = write_silver_parquet(clients_clean, "clients.parquet")
    silver_achats = write_silver_parquet(achats_clean, "achats.parquet")

    log_event(
        "FLOW_COMPLETE",
        flow="silver_transformation",
        clients_rows=len(clients_clean),
        achats_rows=len(achats_clean)
    )

    return {
        "clients": silver_clients,
        "achats": silver_achats
    }


if __name__ == "__main__":
    result = silver_transformation_flow()
    print(f"Silver transformation complete: {result}")
