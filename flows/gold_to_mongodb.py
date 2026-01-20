from io import BytesIO

import pandas as pd
from prefect import flow, task

from config import (
    BUCKET_GOLD,
    get_minio_client,
    get_mongodb_database,
    configure_prefect,
)

COLLECTIONS_MAPPING = {
    "revenue_by_country.parquet": "revenue_by_country",
    "revenue_by_product.parquet": "revenue_by_product",
    "monthly_revenue.parquet": "monthly_revenue",
    "customer_metrics.parquet": "customer_metrics",
}


@task(name="read_gold_parquet")
def read_gold_parquet(file_name: str) -> pd.DataFrame:
    client = get_minio_client()
    response = client.get_object(BUCKET_GOLD, file_name)
    df = pd.read_parquet(BytesIO(response.read()))
    response.close()
    response.release_conn()
    print(f"Read {len(df)} rows from {file_name}")
    return df


@task(name="write_to_mongodb")
def write_to_mongodb(df: pd.DataFrame, collection_name: str) -> int:
    db = get_mongodb_database()
    collection = db[collection_name]

    collection.delete_many({})

    records = df.to_dict(orient="records")

    if records:
        collection.insert_many(records)
        print(f"Inserted {len(records)} documents into {collection_name}")

    return len(records)


@flow(name="gold_to_mongodb")
def gold_to_mongodb_flow():
    total_records = 0

    for parquet_file, collection_name in COLLECTIONS_MAPPING.items():
        try:
            df = read_gold_parquet(parquet_file)
            count = write_to_mongodb(df, collection_name)
            total_records += count
        except Exception as e:
            print(f"Error processing {parquet_file}: {e}")

    print(f"Total records loaded into MongoDB: {total_records}")
    return total_records


if __name__ == "__main__":
    configure_prefect()
    gold_to_mongodb_flow()
