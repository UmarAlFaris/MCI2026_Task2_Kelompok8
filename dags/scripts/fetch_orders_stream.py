import os
from datetime import datetime, timezone

import requests
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    LongType,
    IntegerType,
    DoubleType,
    StringType,
    TimestampType,
)


API_URL = "http://96.9.212.102:8000/orders"
OUTPUT_BASE_PATH = "/opt/airflow/data_lake/orders"


def create_spark_session():
    return (
        SparkSession.builder
        .appName("Fetch_Orders_API_To_Data_Lake")
        .master("local[*]")
        .config("spark.driver.memory", "1g")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def get_schema():
    return StructType([
        StructField("batch_id", StringType(), False),
        StructField("ingested_at", TimestampType(), False),

        StructField("order_id", LongType(), True),
        StructField("user_id", LongType(), True),
        StructField("order_number", IntegerType(), True),
        StructField("order_dow", IntegerType(), True),
        StructField("order_hour_of_day", IntegerType(), True),
        StructField("days_since_prior_order", DoubleType(), True),
        StructField("eval_set", StringType(), True),

        StructField("product_id", LongType(), True),
        StructField("product_name", StringType(), True),
        StructField("aisle_id", IntegerType(), True),
        StructField("aisle", StringType(), True),
        StructField("department_id", IntegerType(), True),
        StructField("department", StringType(), True),
        StructField("add_to_cart_order", IntegerType(), True),
        StructField("reordered", IntegerType(), True),
    ])


def flatten_orders(payload, batch_id, ingested_at):
    rows = []

    orders = payload.get("orders", [])

    for order in orders:
        products = order.get("products", [])

        for product in products:
            rows.append({
                "batch_id": batch_id,
                "ingested_at": ingested_at,

                "order_id": order.get("order_id"),
                "user_id": order.get("user_id"),
                "order_number": order.get("order_number"),
                "order_dow": order.get("order_dow"),
                "order_hour_of_day": order.get("order_hour_of_day"),
                "days_since_prior_order": order.get("days_since_prior_order"),
                "eval_set": order.get("eval_set"),

                "product_id": product.get("product_id"),
                "product_name": product.get("product_name"),
                "aisle_id": product.get("aisle_id"),
                "aisle": product.get("aisle"),
                "department_id": product.get("department_id"),
                "department": product.get("department"),
                "add_to_cart_order": product.get("add_to_cart_order"),
                "reordered": product.get("reordered"),
            })

    return rows


def fetch_orders():
    print("Fetching orders from API...")

    response = requests.get(API_URL, timeout=30)
    response.raise_for_status()

    payload = response.json()

    batch_id = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    ingested_at = datetime.now(timezone.utc).replace(tzinfo=None)

    rows = flatten_orders(payload, batch_id, ingested_at)

    if not rows:
        raise ValueError("No rows were created. API response may be empty or structure changed.")

    print(f"Flattened {len(rows)} product rows from API response.")
    print(f"Batch ID: {batch_id}")

    spark = create_spark_session()

    try:
        schema = get_schema()
        df = spark.createDataFrame(rows, schema=schema)

        os.makedirs(OUTPUT_BASE_PATH, exist_ok=True)

        output_path = f"{OUTPUT_BASE_PATH}/orders_{batch_id}.parquet"

        print("Spark schema:")
        df.printSchema()

        print("Sample rows:")
        df.show(5, truncate=False)

        print(f"Saving Parquet to: {output_path}")

        df.coalesce(1).write.mode("overwrite").parquet(output_path)

        print(f"Saved {df.count()} rows to {output_path}")
        print("Phase 2 extraction completed successfully.")

    finally:
        spark.stop()


if __name__ == "__main__":
    fetch_orders()
