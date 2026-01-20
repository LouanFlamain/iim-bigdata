import pandera as pa
from pandera import Column, Check

CLIENTS_SCHEMA = pa.DataFrameSchema(
    {
        "id_client": Column(int, Check.greater_than(0), unique=True, nullable=False),
        "nom": Column(str, nullable=False),
        "email": Column(str, Check.str_matches(r"^[\w\.-]+@[\w\.-]+\.\w+$"), nullable=False),
        "date_inscription": Column(str, nullable=False),
        "pays": Column(str, nullable=False),
    },
    coerce=True,
)

ACHATS_SCHEMA = pa.DataFrameSchema(
    {
        "id_achat": Column(int, Check.greater_than(0), unique=True, nullable=False),
        "id_client": Column(int, Check.greater_than(0), nullable=False),
        "date_achat": Column(str, nullable=False),
        "montant": Column(float, Check.greater_than(0), nullable=False),
        "produit": Column(str, nullable=False),
    },
    coerce=True,
)
