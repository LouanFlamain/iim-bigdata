from io import BytesIO
from datetime import datetime

import pandas as pd
from prefect import flow, task

from config import (
    BUCKET_GOLD,
    get_minio_client,
    get_mongodb_database,
    configure_prefect,
)


ML_COLLECTIONS_MAPPING = {
    "customer_segments.parquet": "customer_segments",
    "churn_predictions.parquet": "churn_predictions",
    "clv_predictions.parquet": "clv_predictions",
    "ml_model_metrics.parquet": "ml_model_metrics",
}


def log_event(event: str, **kwargs):
    timestamp = datetime.now().isoformat()
    details = " | ".join(f"{k}={v}" for k, v in kwargs.items())
    print(f"[{timestamp}] {event} | {details}")


@task(name="read_ml_parquet")
def read_ml_parquet(file_name: str) -> pd.DataFrame:
    client = get_minio_client()
    response = client.get_object(BUCKET_GOLD, file_name)
    df = pd.read_parquet(BytesIO(response.read()))
    response.close()
    response.release_conn()
    log_event("READ_ML_PARQUET", file=file_name, rows=len(df))
    return df


@task(name="write_ml_to_mongodb")
def write_ml_to_mongodb(df: pd.DataFrame, collection_name: str) -> int:
    db = get_mongodb_database()
    collection = db[collection_name]

    collection.delete_many({})

    records = df.to_dict(orient="records")

    if records:
        collection.insert_many(records)
        log_event("WRITE_ML_MONGODB", collection=collection_name, documents=len(records))

    return len(records)


@flow(name="ML to MongoDB Flow")
def ml_to_mongodb_flow() -> dict:
    log_event("FLOW_START", flow="ml_to_mongodb")

    total_records = 0
    results = {}

    for parquet_file, collection_name in ML_COLLECTIONS_MAPPING.items():
        try:
            df = read_ml_parquet(parquet_file)
            count = write_ml_to_mongodb(df, collection_name)
            total_records += count
            results[collection_name] = count
        except Exception as e:
            log_event("ERROR", file=parquet_file, error=str(e)[:200])
            results[collection_name] = 0

    log_event("FLOW_COMPLETE", flow="ml_to_mongodb", total_records=total_records)

    return results


if __name__ == "__main__":
    configure_prefect()
    result = ml_to_mongodb_flow()
    print(f"ML to MongoDB complete: {result}")
