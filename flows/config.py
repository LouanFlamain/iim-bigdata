import os
from pathlib import Path

from dotenv import load_dotenv
from minio import Minio
from pymongo import MongoClient

load_dotenv()

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_SECURE = os.getenv("MINIO_SECURE", "False").lower() == "true"

MONGODB_URI = os.getenv("MONGODB_URI", "mongodb://admin:admin@localhost:27017/")
MONGODB_DB = os.getenv("MONGODB_DB", "analytics")

SQLITE_DB_PATH = os.getenv("SQLITE_DB_PATH", "./data/database/analytics.db")

PREFECT_API_URL = os.getenv("PREFECT_API_URL", "http://localhost:4200/api")

API_URL = os.getenv("API_URL", "http://localhost:8000")

BUCKET_SOURCES = "sources"
BUCKET_BRONZE = "bronze"
BUCKET_SILVER = "silver"
BUCKET_GOLD = "gold"

def get_minio_client() -> Minio:
    return Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=MINIO_SECURE
    )

def get_mongodb_client() -> MongoClient:
    return MongoClient(MONGODB_URI)

def get_mongodb_database():
    client = get_mongodb_client()
    return client[MONGODB_DB]

def configure_prefect() -> None:
    os.environ["PREFECT_API_URL"] = PREFECT_API_URL

if __name__ == "__main__":
    client = get_minio_client()
    print(client.list_buckets())

    configure_prefect()
    print(os.getenv("PREFECT_API_URL"))
