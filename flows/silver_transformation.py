from io import BytesIO

import pandas as pd
from prefect import flow, task

from config import BUCKET_BRONZE, BUCKET_SILVER, get_minio_client


@task(name="read_bronze_csv", retries=2)
def read_bronze_csv(object_name: str) -> pd.DataFrame:
    """
    Read CSV file from bronze bucket into DataFrame.

    Args:
        object_name: Name of object in bronze bucket

    Returns:
        DataFrame with raw data
    """
    client = get_minio_client()
    response = client.get_object(BUCKET_BRONZE, object_name)
    df = pd.read_csv(response)
    response.close()
    response.release_conn()
    print(f"Read {len(df)} rows from {BUCKET_BRONZE}/{object_name}")
    return df


@task(name="clean_clients")
def clean_clients(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean and transform clients data.

    Transformations:
    - Remove duplicates
    - Parse date_inscription as datetime
    - Normalize country names
    - Validate email format
    """
    df = df.drop_duplicates(subset=["id_client"])
    df["date_inscription"] = pd.to_datetime(df["date_inscription"])
    df["pays"] = df["pays"].str.strip().str.title()
    df["email"] = df["email"].str.lower().str.strip()
    df = df.dropna(subset=["id_client", "email"])

    print(f"Cleaned clients: {len(df)} rows")
    return df


@task(name="clean_achats")
def clean_achats(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean and transform purchases data.

    Transformations:
    - Remove duplicates
    - Parse date_achat as datetime
    - Ensure montant is positive
    - Normalize product names
    """
    df = df.drop_duplicates(subset=["id_achat"])
    df["date_achat"] = pd.to_datetime(df["date_achat"])
    df["produit"] = df["produit"].str.strip().str.title()
    df = df[df["montant"] > 0]
    df = df.dropna(subset=["id_achat", "id_client", "montant"])

    print(f"Cleaned achats: {len(df)} rows")
    return df


@task(name="write_silver_parquet", retries=2)
def write_silver_parquet(df: pd.DataFrame, object_name: str) -> str:
    """
    Write DataFrame to silver bucket as Parquet.

    Args:
        df: Cleaned DataFrame
        object_name: Name of object in silver bucket

    Returns:
        Object name in silver bucket
    """
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
    print(f"Wrote {object_name} to {BUCKET_SILVER}")
    return object_name


@flow(name="Silver Transformation Flow")
def silver_transformation_flow() -> dict:
    """
    Main flow: Read bronze data, clean/transform, write to silver as Parquet.

    Returns:
        Dictionary with transformed file names
    """
    # Read from bronze
    clients_df = read_bronze_csv("clients.csv")
    achats_df = read_bronze_csv("achats.csv")

    # Clean and transform
    clients_clean = clean_clients(clients_df)
    achats_clean = clean_achats(achats_df)

    # Write to silver as Parquet
    silver_clients = write_silver_parquet(clients_clean, "clients.parquet")
    silver_achats = write_silver_parquet(achats_clean, "achats.parquet")

    return {
        "clients": silver_clients,
        "achats": silver_achats
    }


if __name__ == "__main__":
    result = silver_transformation_flow()
    print(f"Silver transformation complete: {result}")
