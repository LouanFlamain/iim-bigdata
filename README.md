# Big Data ELT Pipeline

Pipeline ELT Big Data avec architecture medallion (Bronze/Silver/Gold), Machine Learning (segmentation RFM, churn prediction, CLV), MinIO, MongoDB, API Flask, Metabase et Dashboard Streamlit.

## Architecture

```
CSV Sources
    ↓ (validation Pandera + retry)
Bronze (MinIO) → Silver (MinIO) → Gold (MinIO)
                                    ↓
                            ML Pipeline (Prefect)
                                    ↓
                            Gold ML (4 parquets)
                                    ↓
                              MongoDB (8 collections)
                                    ↓
                    ┌───────────────┼───────────────┐
                    ↓               ↓               ↓
              API Flask      Metabase:3000    Streamlit
              (REST)         (dashboards)    (Plotly + ML)
```

## Installation

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Demarrage des services

```bash
docker compose up -d
```

| Service | URL | Credentials |
|---------|-----|-------------|
| MinIO Console | http://localhost:9001 | minioadmin / minioadmin |
| Prefect UI | http://localhost:4200 | - |
| MongoDB | localhost:27017 | admin / admin |
| API Flask | http://localhost:8000 | - |
| Metabase | http://localhost:3000 | (configuration initiale requise) |
| Streamlit | http://localhost:8501 | - |

## Utilisation

### 1. Generer les donnees

```bash
python scripts/generate_data.py
```

### 2. Executer le pipeline de donnees

```bash
python flows/bronze_ingestion.py      # Bronze : ingestion CSV + validation
python flows/silver_transformation.py  # Silver : nettoyage + integrite referentielle
python flows/gold_aggregation.py       # Gold : agregations analytiques
python flows/gold_to_mongodb.py        # Export Gold vers MongoDB
```

### 3. Executer le pipeline ML

```bash
python flows/ml_pipeline.py           # Entrainement des modeles + predictions
python flows/ml_to_mongodb.py         # Export ML vers MongoDB
```

### 4. Tester l'API

```bash
# Health check
curl http://localhost:8000/health

# Analytics
curl http://localhost:8000/revenue/country
curl http://localhost:8000/revenue/product
curl http://localhost:8000/revenue/monthly
curl http://localhost:8000/customers
curl http://localhost:8000/customers/1
curl http://localhost:8000/kpis

# Machine Learning
curl http://localhost:8000/ml/segments
curl http://localhost:8000/ml/segments/summary
curl http://localhost:8000/ml/churn
curl http://localhost:8000/ml/churn/high-risk
curl http://localhost:8000/ml/clv
curl http://localhost:8000/ml/model-metrics
```

### 5. Lancer le Dashboard

```bash
streamlit run dashboard/app.py
```

Ouvrir http://localhost:8501

### 6. Configurer Metabase

1. Acceder a http://localhost:3000
2. Creer un compte administrateur
3. Ajouter MongoDB comme source de donnees :
   - Type: MongoDB
   - Host: mongodb
   - Port: 27017
   - Database: analytics
   - Username: admin
   - Password: admin

## Structure du projet

```
.
├── api/
│   └── app.py                  # API Flask REST
├── dashboard/
│   └── app.py                  # Dashboard Streamlit + Plotly
├── flows/
│   ├── config.py               # Configuration centrale
│   ├── schemas.py              # Schemas Pandera (validation)
│   ├── bronze_ingestion.py     # Pipeline Bronze
│   ├── silver_transformation.py # Pipeline Silver
│   ├── gold_aggregation.py     # Pipeline Gold
│   ├── gold_to_mongodb.py      # Export Gold → MongoDB
│   ├── ml_features.py          # Feature engineering ML
│   ├── ml_models.py            # Modeles ML
│   ├── ml_pipeline.py          # Pipeline ML
│   └── ml_to_mongodb.py        # Export ML → MongoDB
├── scripts/
│   └── generate_data.py        # Generateur de donnees
├── data/
│   └── sources/                # Fichiers CSV generes
├── docker-compose.yml
├── Dockerfile.api
└── requirements.txt
```

## Data Lake (MinIO)

| Bucket | Format | Description |
|--------|--------|-------------|
| sources | CSV | Fichiers bruts uploades |
| bronze | CSV | Copie brute des sources |
| silver | Parquet | Donnees nettoyees et validees |
| gold | Parquet | Agregations analytiques + predictions ML |

## Collections MongoDB

| Collection | Source | Description |
|------------|--------|-------------|
| revenue_by_country | Gold | Revenus agreges par pays |
| revenue_by_product | Gold | Revenus agreges par produit |
| monthly_revenue | Gold | Evolution mensuelle des revenus |
| customer_metrics | Gold | Metriques clients (CA, panier moyen, nb achats) |
| customer_segments | ML | Segmentation RFM (Champions/Loyal/At Risk/Lost) |
| churn_predictions | ML | Probabilite de churn + niveau de risque |
| clv_predictions | ML | CLV predite sur 12 mois |
| ml_model_metrics | ML | Metriques de performance des modeles |

## Modeles Machine Learning

| Modele | Algorithme | Features | Output |
|--------|------------|----------|--------|
| Segmentation | StandardScaler + K-Means (4 clusters) | recency, frequency, monetary | segment_id, segment_name |
| Churn | RandomForestClassifier | days_since_last, frequency, avg_basket, tenure | churn_probability, churn_risk_level |
| CLV | GradientBoostingRegressor | avg_purchase, frequency, customer_age | predicted_clv_12m, clv_segment |

## Robustesse

- **Validation Pandera** : schemas pour clients et achats
- **Retry avec backoff** : [2s, 10s, 30s] sur les operations MinIO/MongoDB
- **Logging structure** : timestamps + evenements + metriques
- **Integrite referentielle** : verification des id_client dans achats

## Dashboard Streamlit

Sections disponibles :
- **KPIs Globaux** : CA total, nombre d'achats, clients, panier moyen
- **Revenus par Pays** : bar chart horizontal + tableau
- **Revenus par Produit** : pie chart + tableau
- **Evolution Mensuelle** : line charts CA et achats
- **Analyse Clients** : top 10, repartition par pays
- **Segmentation ML** : scatter 3D RFM, pie chart segments, stats
- **Analyse Churn** : gauge risque global, distribution probabilites, clients high-risk
- **Customer Lifetime Value** : histogramme CLV, scatter actuel vs predit, top 10
- **Metriques Modeles** : tableau des performances

## Schema des donnees

**clients.csv**
| Colonne | Type | Description |
|---------|------|-------------|
| id_client | int | Identifiant unique |
| nom | str | Nom complet |
| email | str | Adresse email |
| date_inscription | date | Date d'inscription |
| pays | str | Pays de residence |

**achats.csv**
| Colonne | Type | Description |
|---------|------|-------------|
| id_achat | int | Identifiant unique |
| id_client | int | Reference client |
| date_achat | date | Date de l'achat |
| montant | float | Montant en euros |
| produit | str | Nom du produit |
