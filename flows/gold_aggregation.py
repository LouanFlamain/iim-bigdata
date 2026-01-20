from io import BytesIO

import pandas as pd
from prefect import flow, task

from config import BUCKET_GOLD, BUCKET_SILVER, get_minio_client


@task(name="read_silver_parquet", retries=2)
def read_silver_parquet(object_name: str) -> pd.DataFrame:
    """
    Read Parquet file from silver bucket into DataFrame.

    Args:
        object_name: Name of object in silver bucket

    Returns:
        DataFrame with cleaned data
    """
    client = get_minio_client()
    response = client.get_object(BUCKET_SILVER, object_name)
    df = pd.read_parquet(BytesIO(response.read()))
    response.close()
    response.release_conn()
    print(f"Read {len(df)} rows from {BUCKET_SILVER}/{object_name}")
    return df


@task(name="aggregate_revenue_by_country")
def aggregate_revenue_by_country(clients_df: pd.DataFrame, achats_df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate total revenue and purchase count by country.
    """
    merged = achats_df.merge(clients_df[["id_client", "pays"]], on="id_client", how="left")

    agg = merged.groupby("pays").agg(
        total_revenue=("montant", "sum"),
        total_purchases=("id_achat", "count"),
        avg_basket=("montant", "mean")
    ).reset_index()

    agg["total_revenue"] = agg["total_revenue"].round(2)
    agg["avg_basket"] = agg["avg_basket"].round(2)

    print(f"Revenue by country: {len(agg)} countries")
    return agg


@task(name="aggregate_revenue_by_product")
def aggregate_revenue_by_product(achats_df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate total revenue and purchase count by product.
    """
    agg = achats_df.groupby("produit").agg(
        total_revenue=("montant", "sum"),
        total_sales=("id_achat", "count"),
        avg_price=("montant", "mean")
    ).reset_index()

    agg["total_revenue"] = agg["total_revenue"].round(2)
    agg["avg_price"] = agg["avg_price"].round(2)
    agg = agg.sort_values("total_revenue", ascending=False)

    print(f"Revenue by product: {len(agg)} products")
    return agg


@task(name="aggregate_monthly_revenue")
def aggregate_monthly_revenue(achats_df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate revenue by month for trend analysis.
    """
    achats_df["month"] = achats_df["date_achat"].dt.to_period("M").astype(str)

    agg = achats_df.groupby("month").agg(
        total_revenue=("montant", "sum"),
        total_purchases=("id_achat", "count"),
        avg_basket=("montant", "mean")
    ).reset_index()

    agg["total_revenue"] = agg["total_revenue"].round(2)
    agg["avg_basket"] = agg["avg_basket"].round(2)
    agg = agg.sort_values("month")

    print(f"Monthly revenue: {len(agg)} months")
    return agg


@task(name="aggregate_customer_metrics")
def aggregate_customer_metrics(clients_df: pd.DataFrame, achats_df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate metrics per customer: total spent, purchase count, average basket.
    """
    customer_agg = achats_df.groupby("id_client").agg(
        total_spent=("montant", "sum"),
        purchase_count=("id_achat", "count"),
        avg_basket=("montant", "mean"),
        first_purchase=("date_achat", "min"),
        last_purchase=("date_achat", "max")
    ).reset_index()

    merged = clients_df.merge(customer_agg, on="id_client", how="left")
    merged["total_spent"] = merged["total_spent"].fillna(0).round(2)
    merged["purchase_count"] = merged["purchase_count"].fillna(0).astype(int)
    merged["avg_basket"] = merged["avg_basket"].round(2)

    print(f"Customer metrics: {len(merged)} customers")
    return merged


@task(name="write_gold_parquet", retries=2)
def write_gold_parquet(df: pd.DataFrame, object_name: str) -> str:
    """
    Write DataFrame to gold bucket as Parquet.

    Args:
        df: Aggregated DataFrame
        object_name: Name of object in gold bucket

    Returns:
        Object name in gold bucket
    """
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
    print(f"Wrote {object_name} to {BUCKET_GOLD}")
    return object_name


@flow(name="Gold Aggregation Flow")
def gold_aggregation_flow() -> dict:
    """
    Main flow: Read silver data, compute aggregations, write to gold.

    Returns:
        Dictionary with aggregated file names
    """
    # Read from silver
    clients_df = read_silver_parquet("clients.parquet")
    achats_df = read_silver_parquet("achats.parquet")

    # Compute aggregations
    revenue_by_country = aggregate_revenue_by_country(clients_df, achats_df)
    revenue_by_product = aggregate_revenue_by_product(achats_df)
    monthly_revenue = aggregate_monthly_revenue(achats_df)
    customer_metrics = aggregate_customer_metrics(clients_df, achats_df)

    # Write to gold
    gold_files = {
        "revenue_by_country": write_gold_parquet(revenue_by_country, "revenue_by_country.parquet"),
        "revenue_by_product": write_gold_parquet(revenue_by_product, "revenue_by_product.parquet"),
        "monthly_revenue": write_gold_parquet(monthly_revenue, "monthly_revenue.parquet"),
        "customer_metrics": write_gold_parquet(customer_metrics, "customer_metrics.parquet"),
    }

    return gold_files


if __name__ == "__main__":
    result = gold_aggregation_flow()
    print(f"Gold aggregation complete: {result}")
