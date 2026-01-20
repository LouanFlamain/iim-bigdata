import time
from io import BytesIO

import pandas as pd
import requests
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go

import sys
sys.path.append("./flows")
from config import BUCKET_GOLD, get_minio_client, API_URL


st.set_page_config(
    page_title="Analytics Dashboard",
    page_icon="📊",
    layout="wide"
)


def load_gold_data_minio(object_name: str) -> pd.DataFrame:
    client = get_minio_client()
    response = client.get_object(BUCKET_GOLD, object_name)
    df = pd.read_parquet(BytesIO(response.read()))
    response.close()
    response.release_conn()
    return df


def load_data_from_api(endpoint: str) -> pd.DataFrame:
    response = requests.get(f"{API_URL}{endpoint}")
    response.raise_for_status()
    return pd.DataFrame(response.json())


def load_all_data_minio():
    return {
        "revenue_by_country": load_gold_data_minio("revenue_by_country.parquet"),
        "revenue_by_product": load_gold_data_minio("revenue_by_product.parquet"),
        "monthly_revenue": load_gold_data_minio("monthly_revenue.parquet"),
        "customer_metrics": load_gold_data_minio("customer_metrics.parquet"),
    }


def load_all_data_api():
    return {
        "revenue_by_country": load_data_from_api("/revenue/country"),
        "revenue_by_product": load_data_from_api("/revenue/product"),
        "monthly_revenue": load_data_from_api("/revenue/monthly"),
        "customer_metrics": load_data_from_api("/customers"),
    }


def load_ml_data_minio():
    try:
        return {
            "segments": load_gold_data_minio("customer_segments.parquet"),
            "churn": load_gold_data_minio("churn_predictions.parquet"),
            "clv": load_gold_data_minio("clv_predictions.parquet"),
            "metrics": load_gold_data_minio("ml_model_metrics.parquet"),
        }
    except Exception:
        return None


def load_ml_data_api():
    try:
        return {
            "segments": load_data_from_api("/ml/segments"),
            "churn": load_data_from_api("/ml/churn"),
            "clv": load_data_from_api("/ml/clv"),
            "metrics": load_data_from_api("/ml/model-metrics"),
        }
    except Exception:
        return None


def main():
    st.title("Dashboard Analytics")
    st.markdown("Donnees agregees depuis la couche **Gold** du Data Lake")

    st.sidebar.header("Configuration")
    data_source = st.sidebar.radio(
        "Source de donnees",
        ["MinIO (Parquet)", "API (MongoDB)"],
        help="Choisir la source de donnees pour le dashboard"
    )

    compare_performance = st.sidebar.checkbox(
        "Comparer les performances",
        help="Mesurer le temps de chargement des deux sources"
    )

    try:
        if compare_performance:
            st.sidebar.subheader("Performance")

            start_minio = time.time()
            try:
                data_minio = load_all_data_minio()
                minio_time = (time.time() - start_minio) * 1000
                minio_status = "OK"
            except Exception as e:
                minio_time = None
                minio_status = f"Erreur: {e}"
                data_minio = None

            start_api = time.time()
            try:
                data_api = load_all_data_api()
                api_time = (time.time() - start_api) * 1000
                api_status = "OK"
            except Exception as e:
                api_time = None
                api_status = f"Erreur: {e}"
                data_api = None

            st.sidebar.markdown("**Temps de chargement:**")

            if minio_time is not None:
                st.sidebar.metric("MinIO (Parquet)", f"{minio_time:.1f} ms", delta=None)
            else:
                st.sidebar.error(f"MinIO: {minio_status}")

            if api_time is not None:
                st.sidebar.metric("API (MongoDB)", f"{api_time:.1f} ms", delta=None)
            else:
                st.sidebar.error(f"API: {api_status}")

            if minio_time is not None and api_time is not None:
                diff = api_time - minio_time
                diff_pct = (diff / minio_time) * 100 if minio_time > 0 else 0
                if diff > 0:
                    st.sidebar.info(f"API est {diff:.1f} ms ({diff_pct:.1f}%) plus lente")
                else:
                    st.sidebar.info(f"API est {-diff:.1f} ms ({-diff_pct:.1f}%) plus rapide")

            if data_source == "MinIO (Parquet)" and data_minio:
                data = data_minio
            elif data_source == "API (MongoDB)" and data_api:
                data = data_api
            elif data_minio:
                data = data_minio
            elif data_api:
                data = data_api
            else:
                raise Exception("Aucune source de donnees disponible")
        else:
            start_time = time.time()

            if data_source == "MinIO (Parquet)":
                data = load_all_data_minio()
            else:
                data = load_all_data_api()

            load_time = (time.time() - start_time) * 1000
            st.sidebar.metric("Temps de chargement", f"{load_time:.1f} ms")

        revenue_by_country = data["revenue_by_country"]
        revenue_by_product = data["revenue_by_product"]
        monthly_revenue = data["monthly_revenue"]
        customer_metrics = data["customer_metrics"]

        if data_source == "MinIO (Parquet)":
            ml_data = load_ml_data_minio()
        else:
            ml_data = load_ml_data_api()

    except Exception as e:
        st.error(f"Erreur de chargement des donnees: {e}")
        st.info("Assurez-vous que Docker est lance et que les pipelines ont ete executes.")
        return

    st.header("KPIs Globaux")

    total_revenue = revenue_by_country["total_revenue"].sum()
    total_purchases = revenue_by_country["total_purchases"].sum()
    total_customers = len(customer_metrics)
    avg_basket = total_revenue / total_purchases if total_purchases > 0 else 0

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Chiffre d'Affaires Total", f"{total_revenue:,.2f} EUR")
    col2.metric("Nombre d'Achats", f"{total_purchases:,}")
    col3.metric("Nombre de Clients", f"{total_customers:,}")
    col4.metric("Panier Moyen", f"{avg_basket:.2f} EUR")

    st.divider()

    st.header("Revenus par Pays")

    col1, col2 = st.columns([2, 1])

    with col1:
        fig = px.bar(
            revenue_by_country.sort_values("total_revenue", ascending=True),
            x="total_revenue",
            y="pays",
            orientation="h",
            title="Chiffre d'affaires par pays"
        )
        fig.update_layout(showlegend=False)
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.dataframe(
            revenue_by_country.sort_values("total_revenue", ascending=False),
            use_container_width=True,
            hide_index=True
        )

    st.divider()

    st.header("Revenus par Produit")

    col1, col2 = st.columns([2, 1])

    with col1:
        fig = px.pie(
            revenue_by_product,
            values="total_revenue",
            names="produit",
            title="Repartition du CA par produit"
        )
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.dataframe(
            revenue_by_product,
            use_container_width=True,
            hide_index=True
        )

    st.divider()

    st.header("Evolution Mensuelle")

    monthly_revenue_sorted = monthly_revenue.sort_values("month")

    tab1, tab2 = st.tabs(["Chiffre d'Affaires", "Nombre d'Achats"])

    with tab1:
        fig = px.line(
            monthly_revenue_sorted,
            x="month",
            y="total_revenue",
            title="Evolution mensuelle du CA",
            markers=True
        )
        st.plotly_chart(fig, use_container_width=True)

    with tab2:
        fig = px.line(
            monthly_revenue_sorted,
            x="month",
            y="total_purchases",
            title="Evolution mensuelle des achats",
            markers=True
        )
        st.plotly_chart(fig, use_container_width=True)

    st.divider()

    st.header("Analyse Clients")

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Top 10 Clients (CA)")
        top_customers = customer_metrics.nlargest(10, "total_spent")[
            ["nom", "pays", "total_spent", "purchase_count", "avg_basket"]
        ]
        st.dataframe(top_customers, use_container_width=True, hide_index=True)

    with col2:
        st.subheader("Repartition par Pays")
        customers_by_country = customer_metrics.groupby("pays").size().reset_index(name="count")
        fig = px.pie(
            customers_by_country,
            values="count",
            names="pays",
            title="Clients par pays"
        )
        st.plotly_chart(fig, use_container_width=True)

    if ml_data is not None:
        st.divider()
        st.header("Segmentation ML (RFM + K-Means)")

        segments_df = ml_data["segments"]

        col1, col2 = st.columns([2, 1])

        with col1:
            fig = px.scatter_3d(
                segments_df,
                x="recency",
                y="frequency",
                z="monetary",
                color="segment_name",
                title="Segmentation 3D RFM",
                hover_data=["id_client"]
            )
            fig.update_layout(height=500)
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            segment_counts = segments_df["segment_name"].value_counts().reset_index()
            segment_counts.columns = ["segment_name", "count"]
            fig = px.pie(
                segment_counts,
                values="count",
                names="segment_name",
                title="Distribution des segments"
            )
            st.plotly_chart(fig, use_container_width=True)

        st.subheader("Statistiques par segment")
        segment_stats = segments_df.groupby("segment_name").agg(
            nb_clients=("id_client", "count"),
            recency_moy=("recency", "mean"),
            frequency_moy=("frequency", "mean"),
            monetary_moy=("monetary", "mean")
        ).round(2).reset_index()
        st.dataframe(segment_stats, use_container_width=True, hide_index=True)

        st.divider()
        st.header("Analyse Churn")

        churn_df = ml_data["churn"]

        col1, col2, col3 = st.columns(3)

        high_risk_count = len(churn_df[churn_df["churn_risk_level"] == "High"])
        medium_risk_count = len(churn_df[churn_df["churn_risk_level"] == "Medium"])
        low_risk_count = len(churn_df[churn_df["churn_risk_level"] == "Low"])
        total = len(churn_df)

        with col1:
            fig = go.Figure(go.Indicator(
                mode="gauge+number",
                value=high_risk_count / total * 100 if total > 0 else 0,
                title={"text": "Risque Churn Global (%)"},
                gauge={
                    "axis": {"range": [0, 100]},
                    "bar": {"color": "darkred"},
                    "steps": [
                        {"range": [0, 30], "color": "lightgreen"},
                        {"range": [30, 60], "color": "yellow"},
                        {"range": [60, 100], "color": "salmon"}
                    ]
                }
            ))
            fig.update_layout(height=300)
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            risk_counts = churn_df["churn_risk_level"].value_counts().reset_index()
            risk_counts.columns = ["risk_level", "count"]
            color_map = {"High": "red", "Medium": "orange", "Low": "green"}
            fig = px.bar(
                risk_counts,
                x="risk_level",
                y="count",
                color="risk_level",
                color_discrete_map=color_map,
                title="Repartition par niveau de risque"
            )
            fig.update_layout(showlegend=False)
            st.plotly_chart(fig, use_container_width=True)

        with col3:
            fig = px.histogram(
                churn_df,
                x="churn_probability",
                nbins=20,
                title="Distribution des probabilites de churn"
            )
            st.plotly_chart(fig, use_container_width=True)

        st.subheader("Clients a haut risque")
        high_risk_df = churn_df[churn_df["churn_risk_level"] == "High"][
            ["id_client", "churn_probability", "days_since_last", "frequency", "avg_basket"]
        ].sort_values("churn_probability", ascending=False).head(20)
        st.dataframe(high_risk_df, use_container_width=True, hide_index=True)

        st.divider()
        st.header("Customer Lifetime Value (CLV)")

        clv_df = ml_data["clv"]

        col1, col2 = st.columns(2)

        with col1:
            fig = px.histogram(
                clv_df,
                x="predicted_clv_12m",
                nbins=30,
                title="Distribution des CLV predites (12 mois)"
            )
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            fig = px.scatter(
                clv_df,
                x="historical_clv",
                y="predicted_clv_12m",
                color="clv_segment",
                title="CLV actuelle vs predite",
                hover_data=["id_client"]
            )
            st.plotly_chart(fig, use_container_width=True)

        st.subheader("Top 10 clients par CLV predite")
        top_clv = clv_df.nlargest(10, "predicted_clv_12m")[
            ["id_client", "historical_clv", "predicted_clv_12m", "clv_segment", "avg_purchase", "frequency"]
        ]
        st.dataframe(top_clv, use_container_width=True, hide_index=True)

        st.divider()
        st.header("Metriques des Modeles ML")

        metrics_df = ml_data["metrics"]
        st.dataframe(metrics_df, use_container_width=True, hide_index=True)

    st.divider()

    st.header("Exploration des Donnees")

    datasets = ["revenue_by_country", "revenue_by_product", "monthly_revenue", "customer_metrics"]
    if ml_data is not None:
        datasets.extend(["segments", "churn", "clv", "metrics"])

    dataset = st.selectbox("Selectionner un dataset", datasets)

    data_map = {
        "revenue_by_country": revenue_by_country,
        "revenue_by_product": revenue_by_product,
        "monthly_revenue": monthly_revenue,
        "customer_metrics": customer_metrics
    }
    if ml_data is not None:
        data_map.update({
            "segments": ml_data["segments"],
            "churn": ml_data["churn"],
            "clv": ml_data["clv"],
            "metrics": ml_data["metrics"]
        })

    st.dataframe(data_map[dataset], use_container_width=True, hide_index=True)

    st.divider()
    st.caption(f"Source: {data_source}")


if __name__ == "__main__":
    main()
