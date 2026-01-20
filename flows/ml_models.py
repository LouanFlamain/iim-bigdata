import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
from sklearn.ensemble import RandomForestClassifier, GradientBoostingRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score, silhouette_score, mean_squared_error, r2_score


SEGMENT_NAMES = {
    0: "Champions",
    1: "Loyal",
    2: "At Risk",
    3: "Lost"
}


def train_segmentation_model(rfm_df: pd.DataFrame) -> tuple:
    features = rfm_df[["recency", "frequency", "monetary"]].copy()

    scaler = StandardScaler()
    features_scaled = scaler.fit_transform(features)

    kmeans = KMeans(n_clusters=4, random_state=42, n_init=10)
    clusters = kmeans.fit_predict(features_scaled)

    silhouette = silhouette_score(features_scaled, clusters)

    result_df = rfm_df.copy()
    result_df["segment_id"] = clusters

    cluster_means = result_df.groupby("segment_id").agg({
        "recency": "mean",
        "frequency": "mean",
        "monetary": "mean"
    }).reset_index()

    cluster_means["score"] = (
        -cluster_means["recency"] +
        cluster_means["frequency"] * 10 +
        cluster_means["monetary"] / 100
    )
    cluster_means = cluster_means.sort_values("score", ascending=False).reset_index(drop=True)

    segment_mapping = {}
    for i, row in cluster_means.iterrows():
        segment_mapping[row["segment_id"]] = SEGMENT_NAMES[i]

    result_df["segment_name"] = result_df["segment_id"].map(segment_mapping)

    metrics = {
        "model": "segmentation",
        "algorithm": "KMeans",
        "n_clusters": 4,
        "silhouette_score": silhouette,
        "n_samples": len(rfm_df)
    }

    return result_df, kmeans, scaler, metrics


def train_churn_model(churn_df: pd.DataFrame) -> tuple:
    features = churn_df[["days_since_last", "frequency", "avg_basket", "tenure"]].copy()
    target = churn_df["is_churned"]

    X_train, X_test, y_train, y_test = train_test_split(
        features, target, test_size=0.2, random_state=42, stratify=target
    )

    model = RandomForestClassifier(n_estimators=100, random_state=42, n_jobs=-1)
    model.fit(X_train, y_train)

    y_pred = model.predict(X_test)
    y_proba = model.predict_proba(X_test)[:, 1]

    accuracy = accuracy_score(y_test, y_pred)
    precision = precision_score(y_test, y_pred, zero_division=0)
    recall = recall_score(y_test, y_pred, zero_division=0)
    f1 = f1_score(y_test, y_pred, zero_division=0)

    all_proba = model.predict_proba(features)[:, 1]

    result_df = churn_df.copy()
    result_df["churn_probability"] = all_proba

    def assign_risk_level(prob):
        if prob >= 0.7:
            return "High"
        elif prob >= 0.4:
            return "Medium"
        else:
            return "Low"

    result_df["churn_risk_level"] = result_df["churn_probability"].apply(assign_risk_level)

    metrics = {
        "model": "churn",
        "algorithm": "RandomForestClassifier",
        "accuracy": accuracy,
        "precision": precision,
        "recall": recall,
        "f1_score": f1,
        "n_samples": len(churn_df),
        "n_features": 4
    }

    return result_df, model, metrics


def train_clv_model(clv_df: pd.DataFrame) -> tuple:
    df_with_purchases = clv_df[clv_df["frequency"] > 0].copy()

    features = df_with_purchases[["avg_purchase", "frequency", "customer_age"]].copy()
    target = df_with_purchases["historical_clv"]

    if len(df_with_purchases) < 10:
        result_df = clv_df.copy()
        result_df["predicted_clv_12m"] = result_df["historical_clv"] * 1.2
        result_df["clv_segment"] = pd.cut(
            result_df["predicted_clv_12m"],
            bins=[-np.inf, 100, 500, 1000, np.inf],
            labels=["Low", "Medium", "High", "Premium"]
        )
        metrics = {
            "model": "clv",
            "algorithm": "GradientBoostingRegressor",
            "mse": 0,
            "rmse": 0,
            "r2": 0,
            "n_samples": len(clv_df),
            "note": "insufficient_data_for_training"
        }
        return result_df, None, metrics

    X_train, X_test, y_train, y_test = train_test_split(
        features, target, test_size=0.2, random_state=42
    )

    model = GradientBoostingRegressor(n_estimators=100, random_state=42, max_depth=5)
    model.fit(X_train, y_train)

    y_pred = model.predict(X_test)

    mse = mean_squared_error(y_test, y_pred)
    rmse = np.sqrt(mse)
    r2 = r2_score(y_test, y_pred)

    all_features = clv_df[["avg_purchase", "frequency", "customer_age"]].copy()
    all_predictions = model.predict(all_features)

    growth_factor = 1.2
    result_df = clv_df.copy()
    result_df["predicted_clv_12m"] = all_predictions * growth_factor
    result_df["predicted_clv_12m"] = result_df["predicted_clv_12m"].clip(lower=0)

    result_df["clv_segment"] = pd.cut(
        result_df["predicted_clv_12m"],
        bins=[-np.inf, 100, 500, 1000, np.inf],
        labels=["Low", "Medium", "High", "Premium"]
    )

    metrics = {
        "model": "clv",
        "algorithm": "GradientBoostingRegressor",
        "mse": mse,
        "rmse": rmse,
        "r2": r2,
        "n_samples": len(clv_df),
        "n_features": 3
    }

    return result_df, model, metrics
