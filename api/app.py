import os
from flask import Flask, jsonify
from pymongo import MongoClient

app = Flask(__name__)

MONGODB_URI = os.getenv("MONGODB_URI", "mongodb://admin:admin@localhost:27017/")
MONGODB_DB = os.getenv("MONGODB_DB", "analytics")


def get_db():
    client = MongoClient(MONGODB_URI)
    return client[MONGODB_DB]


def serialize_doc(doc):
    if doc is None:
        return None
    if "_id" in doc:
        doc["_id"] = str(doc["_id"])
    return doc


@app.route("/health", methods=["GET"])
def health():
    try:
        db = get_db()
        db.command("ping")
        return jsonify({"status": "healthy", "database": "connected"})
    except Exception as e:
        return jsonify({"status": "unhealthy", "error": str(e)}), 500


@app.route("/revenue/country", methods=["GET"])
def revenue_by_country():
    db = get_db()
    docs = list(db.revenue_by_country.find())
    return jsonify([serialize_doc(doc) for doc in docs])


@app.route("/revenue/product", methods=["GET"])
def revenue_by_product():
    db = get_db()
    docs = list(db.revenue_by_product.find())
    return jsonify([serialize_doc(doc) for doc in docs])


@app.route("/revenue/monthly", methods=["GET"])
def monthly_revenue():
    db = get_db()
    docs = list(db.monthly_revenue.find())
    return jsonify([serialize_doc(doc) for doc in docs])


@app.route("/customers", methods=["GET"])
def customers():
    db = get_db()
    docs = list(db.customer_metrics.find())
    return jsonify([serialize_doc(doc) for doc in docs])


@app.route("/customers/<int:customer_id>", methods=["GET"])
def customer_detail(customer_id):
    db = get_db()
    doc = db.customer_metrics.find_one({"id_client": customer_id})
    if doc is None:
        return jsonify({"error": "Customer not found"}), 404
    return jsonify(serialize_doc(doc))


@app.route("/kpis", methods=["GET"])
def kpis():
    db = get_db()

    revenue_docs = list(db.revenue_by_country.find())
    total_revenue = sum(doc.get("total_revenue", 0) for doc in revenue_docs)
    total_purchases = sum(doc.get("total_purchases", 0) for doc in revenue_docs)

    total_customers = db.customer_metrics.count_documents({})

    avg_basket = total_revenue / total_purchases if total_purchases > 0 else 0

    return jsonify({
        "total_revenue": total_revenue,
        "total_purchases": total_purchases,
        "total_customers": total_customers,
        "avg_basket": round(avg_basket, 2)
    })


@app.route("/ml/segments", methods=["GET"])
def ml_segments():
    db = get_db()
    docs = list(db.customer_segments.find())
    return jsonify([serialize_doc(doc) for doc in docs])


@app.route("/ml/segments/summary", methods=["GET"])
def ml_segments_summary():
    db = get_db()
    pipeline = [
        {
            "$group": {
                "_id": "$segment_name",
                "count": {"$sum": 1},
                "avg_recency": {"$avg": "$recency"},
                "avg_frequency": {"$avg": "$frequency"},
                "avg_monetary": {"$avg": "$monetary"}
            }
        },
        {"$sort": {"avg_monetary": -1}}
    ]
    results = list(db.customer_segments.aggregate(pipeline))
    for r in results:
        r["segment_name"] = r.pop("_id")
    return jsonify(results)


@app.route("/ml/churn", methods=["GET"])
def ml_churn():
    db = get_db()
    docs = list(db.churn_predictions.find())
    return jsonify([serialize_doc(doc) for doc in docs])


@app.route("/ml/churn/high-risk", methods=["GET"])
def ml_churn_high_risk():
    db = get_db()
    docs = list(db.churn_predictions.find({"churn_risk_level": "High"}))
    return jsonify([serialize_doc(doc) for doc in docs])


@app.route("/ml/clv", methods=["GET"])
def ml_clv():
    db = get_db()
    docs = list(db.clv_predictions.find())
    return jsonify([serialize_doc(doc) for doc in docs])


@app.route("/ml/model-metrics", methods=["GET"])
def ml_model_metrics():
    db = get_db()
    docs = list(db.ml_model_metrics.find())
    return jsonify([serialize_doc(doc) for doc in docs])


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, debug=True)
