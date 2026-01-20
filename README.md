# iim-bigdata

Pipeline ELT Big Data avec architecture medallion (Bronze/Silver/Gold), MongoDB, API Flask et Dashboard Streamlit.

## Architecture

```
CSV Sources
    ↓
Bronze (MinIO) → Silver (MinIO) → Gold (MinIO Parquet)
                                        ↓
                                  MongoDB (collections)
                                        ↓
                                  API Flask (REST)
                                        ↓
                                  Dashboard Streamlit
```

## Installation

```bash
# Créer l'environnement virtuel
python -m venv .venv
source .venv/bin/activate

# Installer les dépendances
pip install -r requirements.txt
```

## Démarrage des services

```bash
# Lancer tous les services Docker
docker compose up -d
```

Services disponibles :
| Service | URL | Credentials |
|---------|-----|-------------|
| MinIO Console | http://localhost:9001 | minioadmin / minioadmin |
| Prefect UI | http://localhost:4200 | - |
| MongoDB | localhost:27017 | admin / admin |
| API Flask | http://localhost:8000 | - |

## Utilisation

### 1. Générer les données

```bash
python scripts/generate_data.py
```

### 2. Exécuter les pipelines

```bash
# Bronze : ingestion des CSV
python flows/bronze_ingestion.py

# Silver : nettoyage des données
python flows/silver_transformation.py

# Gold : agrégations analytiques
python flows/gold_aggregation.py

# MongoDB : charger Gold dans MongoDB
python flows/gold_to_mongodb.py
```

### 3. Tester l'API

```bash
# Health check
curl http://localhost:8000/health

# Revenus par pays
curl http://localhost:8000/revenue/country

# Revenus par produit
curl http://localhost:8000/revenue/product

# Revenus mensuels
curl http://localhost:8000/revenue/monthly

# Métriques clients
curl http://localhost:8000/customers

# Détail d'un client
curl http://localhost:8000/customers/1

# KPIs agrégés
curl http://localhost:8000/kpis
```

### 4. Lancer le Dashboard

```bash
streamlit run dashboard/app.py
```

Le dashboard permet de :
- Choisir la source de données (MinIO direct ou API/MongoDB)
- Comparer les performances des deux sources
- Visualiser les KPIs, revenus par pays/produit, tendances mensuelles
- Analyser et segmenter les clients

## Structure du projet

```
.
├── api/
│   └── app.py              # API Flask
├── dashboard/
│   └── app.py              # Dashboard Streamlit
├── flows/
│   ├── config.py           # Configuration (MinIO, MongoDB, Prefect)
│   ├── bronze_ingestion.py # Pipeline Bronze
│   ├── silver_transformation.py
│   ├── gold_aggregation.py
│   └── gold_to_mongodb.py  # Pipeline Gold → MongoDB
├── scripts/
│   └── generate_data.py    # Générateur de données
├── docker-compose.yml
└── requirements.txt
```

## Collections MongoDB

| Collection | Description |
|------------|-------------|
| `revenue_by_country` | Revenus agrégés par pays |
| `revenue_by_product` | Revenus agrégés par produit |
| `monthly_revenue` | Revenus mensuels |
| `customer_metrics` | Métriques clients (CA, panier moyen, nb achats) |
