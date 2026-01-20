from io import BytesIO
from pathlib import Path
from datetime import datetime

import pandas as pd
from prefect import flow, task

from config import BUCKET_BRONZE, BUCKET_SOURCES, get_minio_client
from schemas import CLIENTS_SCHEMA, ACHATS_SCHEMA


def log_event(event: str, **kwargs):
    timestamp = datetime.now().isoformat()
    details = " | ".join(f"{k}={v}" for k, v in kwargs.items())
    print(f"[{timestamp}] {event} | {details}")


@task(name="validate_csv_schema")
def validate_csv_schema(file_path: str, schema_name: str) -> dict:
    schemas = {
        "clients": CLIENTS_SCHEMA,
        "achats": ACHATS_SCHEMA,
    }
    schema = schemas.get(schema_name)

    df = pd.read_csv(file_path)
    total_rows = len(df)

    log_event("VALIDATION_START", file=file_path, schema=schema_name, rows_read=total_rows)

    if schema is None:
        log_event("VALIDATION_SKIP", file=file_path, reason="no_schema_defined")
        return {"valid": True, "rows_read": total_rows, "rows_valid": total_rows, "rows_dropped": 0}

    try:
        validated_df = schema.validate(df, lazy=True)
        rows_valid = len(validated_df)
        rows_dropped = total_rows - rows_valid
        log_event("VALIDATION_SUCCESS", file=file_path, rows_valid=rows_valid, rows_dropped=rows_dropped)
        return {"valid": True, "rows_read": total_rows, "rows_valid": rows_valid, "rows_dropped": rows_dropped}
    except Exception as e:
        log_event("VALIDATION_FAILED", file=file_path, error=str(e)[:200])
        raise ValueError(f"Schema validation failed for {file_path}: {e}")


@task(name="upload_to_sources", retries=3, retry_delay_seconds=[2, 10, 30])
def upload_csv_to_sources(file_path: str, object_name: str) -> str:
    client = get_minio_client()

    if not client.bucket_exists(BUCKET_SOURCES):
        client.make_bucket(BUCKET_SOURCES)

    client.fput_object(BUCKET_SOURCES, object_name, file_path)
    log_event("UPLOAD_SUCCESS", bucket=BUCKET_SOURCES, object=object_name)
    return object_name


@task(name="copy_to_bronze", retries=3, retry_delay_seconds=[2, 10, 30])
def copy_to_bronze_layer(object_name: str) -> str:
    client = get_minio_client()

    if not client.bucket_exists(BUCKET_BRONZE):
        client.make_bucket(BUCKET_BRONZE)

    response = client.get_object(BUCKET_SOURCES, object_name)
    data = response.read()
    response.close()
    response.release_conn()

    client.put_object(
        BUCKET_BRONZE,
        object_name,
        BytesIO(data),
        length=len(data)
    )
    log_event("COPY_SUCCESS", source=BUCKET_SOURCES, destination=BUCKET_BRONZE, object=object_name)
    return object_name


@flow(name="Bronze Ingestion Flow")
def bronze_ingestion_flow(data_dir: str = "./data/sources") -> dict:
    data_path = Path(data_dir)

    clients_file = str(data_path / "clients.csv")
    achats_file = str(data_path / "achats.csv")

    log_event("FLOW_START", flow="bronze_ingestion", data_dir=data_dir)

    clients_validation = validate_csv_schema(clients_file, "clients")
    achats_validation = validate_csv_schema(achats_file, "achats")

    clients_name = upload_csv_to_sources(clients_file, "clients.csv")
    achats_name = upload_csv_to_sources(achats_file, "achats.csv")

    bronze_clients = copy_to_bronze_layer(clients_name)
    bronze_achats = copy_to_bronze_layer(achats_name)

    log_event(
        "FLOW_COMPLETE",
        flow="bronze_ingestion",
        clients_rows=clients_validation["rows_valid"],
        achats_rows=achats_validation["rows_valid"]
    )

    return {
        "clients": bronze_clients,
        "achats": bronze_achats,
        "validation": {
            "clients": clients_validation,
            "achats": achats_validation
        }
    }


if __name__ == "__main__":
    result = bronze_ingestion_flow()
    print(f"Bronze ingestion complete: {result}")
