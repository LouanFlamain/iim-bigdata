import pandas as pd
import numpy as np
from datetime import datetime


def compute_rfm_features(clients_df: pd.DataFrame, achats_df: pd.DataFrame) -> pd.DataFrame:
    reference_date = achats_df["date_achat"].max() + pd.Timedelta(days=1)

    achats_agg = achats_df.groupby("id_client").agg(
        last_purchase=("date_achat", "max"),
        frequency=("id_achat", "count"),
        monetary=("montant", "sum")
    ).reset_index()

    achats_agg["recency"] = (reference_date - achats_agg["last_purchase"]).dt.days

    rfm_df = clients_df[["id_client"]].merge(achats_agg, on="id_client", how="left")

    rfm_df["recency"] = rfm_df["recency"].fillna(9999)
    rfm_df["frequency"] = rfm_df["frequency"].fillna(0)
    rfm_df["monetary"] = rfm_df["monetary"].fillna(0)

    rfm_df = rfm_df[["id_client", "recency", "frequency", "monetary"]]

    return rfm_df


def compute_churn_features(clients_df: pd.DataFrame, achats_df: pd.DataFrame, churn_days: int = 60) -> pd.DataFrame:
    reference_date = achats_df["date_achat"].max() + pd.Timedelta(days=1)

    achats_agg = achats_df.groupby("id_client").agg(
        last_purchase=("date_achat", "max"),
        first_purchase=("date_achat", "min"),
        frequency=("id_achat", "count"),
        total_spent=("montant", "sum")
    ).reset_index()

    achats_agg["days_since_last"] = (reference_date - achats_agg["last_purchase"]).dt.days
    achats_agg["avg_basket"] = achats_agg["total_spent"] / achats_agg["frequency"]
    achats_agg["tenure"] = (achats_agg["last_purchase"] - achats_agg["first_purchase"]).dt.days

    churn_df = clients_df[["id_client", "date_inscription"]].merge(achats_agg, on="id_client", how="left")

    churn_df["days_since_last"] = churn_df["days_since_last"].fillna(9999)
    churn_df["frequency"] = churn_df["frequency"].fillna(0)
    churn_df["avg_basket"] = churn_df["avg_basket"].fillna(0)
    churn_df["tenure"] = churn_df["tenure"].fillna(0)

    churn_df["is_churned"] = (churn_df["days_since_last"] > churn_days).astype(int)

    churn_df = churn_df[["id_client", "days_since_last", "frequency", "avg_basket", "tenure", "is_churned"]]

    return churn_df


def compute_clv_features(clients_df: pd.DataFrame, achats_df: pd.DataFrame) -> pd.DataFrame:
    reference_date = achats_df["date_achat"].max() + pd.Timedelta(days=1)

    achats_agg = achats_df.groupby("id_client").agg(
        first_purchase=("date_achat", "min"),
        frequency=("id_achat", "count"),
        total_spent=("montant", "sum")
    ).reset_index()

    achats_agg["avg_purchase"] = achats_agg["total_spent"] / achats_agg["frequency"]

    clv_df = clients_df[["id_client", "date_inscription"]].merge(achats_agg, on="id_client", how="left")

    clv_df["customer_age"] = (reference_date - clv_df["date_inscription"]).dt.days

    clv_df["avg_purchase"] = clv_df["avg_purchase"].fillna(0)
    clv_df["frequency"] = clv_df["frequency"].fillna(0)
    clv_df["total_spent"] = clv_df["total_spent"].fillna(0)

    clv_df["historical_clv"] = clv_df["total_spent"]

    clv_df = clv_df[["id_client", "avg_purchase", "frequency", "customer_age", "historical_clv"]]

    return clv_df
