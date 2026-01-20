from io import BytesIO
from datetime import datetime

import pandas as pd
from prefect import flow, task

from config import BUCKET_SILVER, BUCKET_GOLD, get_minio_client
from ml_features import compute_rfm_features, compute_churn_features, compute_clv_features
from ml_models import train_segmentation_model, train_churn_model, train_clv_model


def log_event(event: str, **kwargs):
    timestamp = datetime.now().isoformat()
    details = " | ".join(f"{k}={v}" for k, v in kwargs.items())
    print(f"[{timestamp}] {event} | {details}")


@task(name="read_silver_parquet", retries=2)
def read_silver_parquet(object_name: str) -> pd.DataFrame:
    client = get_minio_client()
    response = client.get_object(BUCKET_SILVER, object_name)
    df = pd.read_parquet(BytesIO(response.read()))
    response.close()
    response.release_conn()
    log_event("READ_SILVER", object=object_name, rows=len(df))
    return df


@task(name="compute_rfm")
def compute_rfm_task(clients_df: pd.DataFrame, achats_df: pd.DataFrame) -> pd.DataFrame:
    rfm_df = compute_rfm_features(clients_df, achats_df)
    log_event("COMPUTE_RFM", rows=len(rfm_df))
    return rfm_df


@task(name="compute_churn")
def compute_churn_task(clients_df: pd.DataFrame, achats_df: pd.DataFrame) -> pd.DataFrame:
    churn_df = compute_churn_features(clients_df, achats_df)
    log_event("COMPUTE_CHURN", rows=len(churn_df))
    return churn_df


@task(name="compute_clv")
def compute_clv_task(clients_df: pd.DataFrame, achats_df: pd.DataFrame) -> pd.DataFrame:
    clv_df = compute_clv_features(clients_df, achats_df)
    log_event("COMPUTE_CLV", rows=len(clv_df))
    return clv_df


@task(name="train_segmentation")
def train_segmentation_task(rfm_df: pd.DataFrame) -> tuple:
    result_df, model, scaler, metrics = train_segmentation_model(rfm_df)
    log_event("TRAIN_SEGMENTATION", silhouette=round(metrics["silhouette_score"], 4))
    return result_df, metrics


@task(name="train_churn")
def train_churn_task(churn_df: pd.DataFrame) -> tuple:
    result_df, model, metrics = train_churn_model(churn_df)
    log_event(
        "TRAIN_CHURN",
        accuracy=round(metrics["accuracy"], 4),
        precision=round(metrics["precision"], 4),
        recall=round(metrics["recall"], 4)
    )
    return result_df, metrics


@task(name="train_clv")
def train_clv_task(clv_df: pd.DataFrame) -> tuple:
    result_df, model, metrics = train_clv_model(clv_df)
    log_event("TRAIN_CLV", r2=round(metrics["r2"], 4), rmse=round(metrics.get("rmse", 0), 2))
    return result_df, metrics


@task(name="write_gold_parquet", retries=2)
def write_gold_parquet(df: pd.DataFrame, object_name: str) -> str:
    client = get_minio_client()

    if not client.bucket_exists(BUCKET_GOLD):
        client.make_bucket(BUCKET_GOLD)

    buffer = BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)

    client.put_object(
        BUCKET_GOLD,
        object_name,
        buffer,
        length=buffer.getbuffer().nbytes,
        content_type="application/octet-stream"
    )
    log_event("WRITE_GOLD", object=object_name, rows=len(df))
    return object_name


@flow(name="ML Pipeline Flow")
def ml_pipeline_flow() -> dict:
    log_event("FLOW_START", flow="ml_pipeline")

    clients_df = read_silver_parquet("clients.parquet")
    achats_df = read_silver_parquet("achats.parquet")

    rfm_df = compute_rfm_task(clients_df, achats_df)
    churn_df = compute_churn_task(clients_df, achats_df)
    clv_df = compute_clv_task(clients_df, achats_df)

    segments_df, seg_metrics = train_segmentation_task(rfm_df)
    churn_pred_df, churn_metrics = train_churn_task(churn_df)
    clv_pred_df, clv_metrics = train_clv_task(clv_df)

    write_gold_parquet(segments_df, "customer_segments.parquet")
    write_gold_parquet(churn_pred_df, "churn_predictions.parquet")
    write_gold_parquet(clv_pred_df, "clv_predictions.parquet")

    all_metrics = []
    for m in [seg_metrics, churn_metrics, clv_metrics]:
        metrics_row = {k: str(v) if not isinstance(v, (int, float)) else v for k, v in m.items()}
        all_metrics.append(metrics_row)

    metrics_df = pd.DataFrame(all_metrics)
    write_gold_parquet(metrics_df, "ml_model_metrics.parquet")

    log_event("FLOW_COMPLETE", flow="ml_pipeline")

    return {
        "segments": len(segments_df),
        "churn_predictions": len(churn_pred_df),
        "clv_predictions": len(clv_pred_df),
        "metrics": all_metrics
    }


if __name__ == "__main__":
    result = ml_pipeline_flow()
    print(f"ML pipeline complete: {result}")
