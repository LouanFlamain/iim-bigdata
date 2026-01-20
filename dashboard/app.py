import time
from io import BytesIO

import pandas as pd
import requests
import streamlit as st

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


def main():
    st.title("📊 Dashboard Analytics")
    st.markdown("Données agrégées depuis la couche **Gold** du Data Lake")

    st.sidebar.header("⚙️ Configuration")
    data_source = st.sidebar.radio(
        "Source de données",
        ["MinIO (Parquet)", "API (MongoDB)"],
        help="Choisir la source de données pour le dashboard"
    )

    compare_performance = st.sidebar.checkbox(
        "Comparer les performances",
        help="Mesurer le temps de chargement des deux sources"
    )

    try:
        if compare_performance:
            st.sidebar.subheader("📈 Performance")

            start_minio = time.time()
            try:
                data_minio = load_all_data_minio()
                minio_time = (time.time() - start_minio) * 1000
                minio_status = "✅"
            except Exception as e:
                minio_time = None
                minio_status = f"❌ {e}"
                data_minio = None

            start_api = time.time()
            try:
                data_api = load_all_data_api()
                api_time = (time.time() - start_api) * 1000
                api_status = "✅"
            except Exception as e:
                api_time = None
                api_status = f"❌ {e}"
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
                raise Exception("Aucune source de données disponible")
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

    except Exception as e:
        st.error(f"Erreur de chargement des données: {e}")
        st.info("Assurez-vous que Docker est lancé et que les pipelines ont été exécutés.")
        return

    st.header("📈 KPIs Globaux")

    total_revenue = revenue_by_country["total_revenue"].sum()
    total_purchases = revenue_by_country["total_purchases"].sum()
    total_customers = len(customer_metrics)
    avg_basket = total_revenue / total_purchases if total_purchases > 0 else 0

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Chiffre d'Affaires Total", f"{total_revenue:,.2f} €")
    col2.metric("Nombre d'Achats", f"{total_purchases:,}")
    col3.metric("Nombre de Clients", f"{total_customers:,}")
    col4.metric("Panier Moyen", f"{avg_basket:.2f} €")

    st.divider()

    st.header("🌍 Revenus par Pays")

    col1, col2 = st.columns([2, 1])

    with col1:
        st.bar_chart(
            revenue_by_country.set_index("pays")["total_revenue"],
            use_container_width=True
        )

    with col2:
        st.dataframe(
            revenue_by_country.sort_values("total_revenue", ascending=False),
            use_container_width=True,
            hide_index=True
        )

    st.divider()

    st.header("📦 Revenus par Produit")

    col1, col2 = st.columns([2, 1])

    with col1:
        st.bar_chart(
            revenue_by_product.set_index("produit")["total_revenue"],
            use_container_width=True
        )

    with col2:
        st.dataframe(
            revenue_by_product,
            use_container_width=True,
            hide_index=True
        )

    st.divider()

    st.header("📅 Évolution Mensuelle")

    monthly_revenue_sorted = monthly_revenue.sort_values("month")

    tab1, tab2 = st.tabs(["Chiffre d'Affaires", "Nombre d'Achats"])

    with tab1:
        st.line_chart(
            monthly_revenue_sorted.set_index("month")["total_revenue"],
            use_container_width=True
        )

    with tab2:
        st.line_chart(
            monthly_revenue_sorted.set_index("month")["total_purchases"],
            use_container_width=True
        )

    st.divider()

    st.header("👥 Analyse Clients")

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Top 10 Clients (CA)")
        top_customers = customer_metrics.nlargest(10, "total_spent")[
            ["nom", "pays", "total_spent", "purchase_count", "avg_basket"]
        ]
        st.dataframe(top_customers, use_container_width=True, hide_index=True)

    with col2:
        st.subheader("Répartition par Pays")
        customers_by_country = customer_metrics.groupby("pays").size().reset_index(name="count")
        st.bar_chart(
            customers_by_country.set_index("pays")["count"],
            use_container_width=True
        )

    st.divider()

    st.header("🎯 Segmentation Clients")

    def segment_customer(total_spent):
        if total_spent >= 2000:
            return "Premium"
        elif total_spent >= 1000:
            return "Regular"
        else:
            return "Occasional"

    customer_metrics["segment"] = customer_metrics["total_spent"].apply(segment_customer)
    segment_counts = customer_metrics["segment"].value_counts()

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Nombre de clients par segment")
        st.bar_chart(segment_counts)

    with col2:
        st.subheader("Détail des segments")
        segment_stats = customer_metrics.groupby("segment").agg(
            nb_clients=("id_client", "count"),
            ca_total=("total_spent", "sum"),
            panier_moyen=("avg_basket", "mean")
        ).round(2)
        st.dataframe(segment_stats, use_container_width=True)

    st.divider()

    st.header("🔍 Exploration des Données")

    dataset = st.selectbox(
        "Sélectionner un dataset",
        ["revenue_by_country", "revenue_by_product", "monthly_revenue", "customer_metrics"]
    )

    data_map = {
        "revenue_by_country": revenue_by_country,
        "revenue_by_product": revenue_by_product,
        "monthly_revenue": monthly_revenue,
        "customer_metrics": customer_metrics
    }

    st.dataframe(data_map[dataset], use_container_width=True, hide_index=True)

    st.divider()
    st.caption(f"Source: {data_source}")


if __name__ == "__main__":
    main()
