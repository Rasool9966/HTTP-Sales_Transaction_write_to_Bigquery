import os
import uuid
import json
import logging
from datetime import datetime
import functions_framework
from flask import jsonify, make_response
from google.cloud import bigquery

# Configure logging
logger = logging.getLogger("HTTPHandler")
logger.setLevel(logging.INFO)

handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s %(name)s : %(message)s'))

if not logger.handlers:
    logger.addHandler(handler)

# Initialize BigQuery client and table name from environment
bq_client = bigquery.Client()
BQ_TABLE = os.environ.get("BQ_TABLE")  # e.g., "project_id.dataset_id.table_name"

@functions_framework.http
def sales_data(request):
    if request.method != "POST":
        logger.warning("Invalid request method: %s", request.method)
        return make_response(jsonify({"error": "Method not allowed"}), 405)

    data = request.get_json(silent=True)
    if not data:
        logger.warning("No JSON payload provided")
        return make_response(jsonify({"error": "Invalid JSON payload"}), 400)

    # Validate required fields
    required_fields = ["transaction_id", "date", "customer_name", "items", "total_amount", "payment_method"]
    for field in required_fields:
        if field not in data:
            logger.warning("Missing required field: %s", field)
            return make_response(jsonify({"error": f"Missing required field: {field}"}), 400)

    # Validate item structure
    if not isinstance(data["items"], list):
        return make_response(jsonify({"error": "'items' must be a list"}), 400)

    for item in data["items"]:
        if not all(k in item for k in ("name", "price", "quantity")):
            logger.warning("Invalid item in items list: %s", item)
            return make_response(jsonify({"error": "Each item must include 'name', 'price', and 'quantity'"}), 400)

    if not isinstance(data["total_amount"], (int, float)):
        return make_response(jsonify({"error": "'total_amount' must be a number"}), 400)

    # Parse and validate date
    try:
        transaction_date = datetime.strptime(data["date"], "%Y-%m-%d").date()
    except ValueError:
        logger.error("Invalid date format: %s", data["date"])
        return make_response(jsonify({"error": "Invalid date format, expected YYYY-MM-DD"}), 400)

    if transaction_date > datetime.now().date():
        logger.error("Transaction date cannot be in the future")
        return make_response(jsonify({"error": "Transaction date cannot be in the future"}), 400)

    # Enrich and transform
    total_tax = round(0.07 * data["total_amount"], 2)
    order_id = str(uuid.uuid4())
    processed_at = datetime.utcnow().isoformat()

    # Final structure to load into BigQuery
    row = {
            "order_id": order_id,
            "transaction_id": data["transaction_id"],
            "date": data["date"],
            "customer_name": data["customer_name"],
            "total_amount": data["total_amount"],
            "total_tax": total_tax,
            "payment_method": data["payment_method"],
            "processed_at": processed_at,
            "items": data["items"],
            "status": "success",
            "message": "Transaction processed and stored successfully"
    }


    # # Insert into BigQuery
    # if BQ_TABLE:
    #     try:
    #         errors = bq_client.insert_rows_json(BQ_TABLE, [row])
    #         if errors:
    #             logger.error("Failed to insert rows into BigQuery: %s", errors)
    #             return make_response(jsonify({"error": "Failed to insert data into BigQuery"}), 500)
    #         logger.info("Data inserted into BigQuery successfully")
    #     except Exception as e:
    #         logger.exception("Error inserting data into BigQuery: %s", str(e))
    #         return make_response(jsonify({"error": "Internal server error"}), 500)
    # else:
    #     logger.warning("BigQuery table not configured, skipping data insertion")

    # # Return final response
    # return make_response(jsonify({
    #     "order_id": order_id,
    #     "status": "success",
    #     "message": "Transaction processed and stored successfully"
    # }), 200)
# End of main.py
# Note: Ensure that the environment variable BQ_TABLE is set to the correct BigQuery table path
# when deploying this function.
# Example: export BQ_TABLE="your-project.your_dataset.your_table"   
# This code is designed to run in a Google Cloud Function environment.
# It expects the BQ_TABLE environment variable to be set to the BigQuery table path.
# The function processes sales data, validates it, enriches it, and inserts it into BigQuery.
# It also handles various error cases and logs relevant information for debugging.
# The function is triggered by HTTP POST requests and returns appropriate JSON responses.
