from io import BytesIO

import pandas as pd
import streamlit as st

import sys
sys.path.append("./flows")
from config import BUCKET_GOLD, get_minio_client


st.set_page_config(
    page_title="Analytics Dashboard",
    page_icon="📊",
    layout="wide"
)


@st.cache_data(ttl=300)
def load_gold_data(object_name: str) -> pd.DataFrame:
    """Load Parquet file from Gold bucket."""
    client = get_minio_client()
    response = client.get_object(BUCKET_GOLD, object_name)
    df = pd.read_parquet(BytesIO(response.read()))
    response.close()
    response.release_conn()
    return df


def main():
    st.title("📊 Dashboard Analytics")
    st.markdown("Données agrégées depuis la couche **Gold** du Data Lake")

    try:
        # Load all gold tables
        revenue_by_country = load_gold_data("revenue_by_country.parquet")
        revenue_by_product = load_gold_data("revenue_by_product.parquet")
        monthly_revenue = load_gold_data("monthly_revenue.parquet")
        customer_metrics = load_gold_data("customer_metrics.parquet")
    except Exception as e:
        st.error(f"Erreur de connexion à MinIO: {e}")
        st.info("Assurez-vous que Docker est lancé et que le pipeline Gold a été exécuté.")
        return

    # --- KPIs ---
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

    # --- Revenue by Country ---
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

    # --- Revenue by Product ---
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

    # --- Monthly Trend ---
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

    # --- Customer Analysis ---
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

    # --- Customer Segmentation ---
    st.header("🎯 Segmentation Clients")

    # Define segments based on total spent
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

    # --- Raw Data Explorer ---
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


if __name__ == "__main__":
    main()
