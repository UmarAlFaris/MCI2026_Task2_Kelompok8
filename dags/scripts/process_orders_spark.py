import glob
import os
import shutil

from clickhouse_driver import Client
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


DATA_LAKE_PATH = "/opt/airflow/data_lake/orders"

CLICKHOUSE_HOST = "clickhouse-server"
CLICKHOUSE_USER = "admin"
CLICKHOUSE_PASSWORD = "rahasia"

DATABASE_NAME = "analytics"
TABLE_NAME = "order_items"


ORDER_ITEMS_COLUMNS = [
    "batch_id",
    "ingested_at",
    "order_id",
    "user_id",
    "order_number",
    "order_dow",
    "order_hour_of_day",
    "days_since_prior_order",
    "eval_set",
    "product_id",
    "product_name",
    "aisle_id",
    "aisle",
    "department_id",
    "department",
    "add_to_cart_order",
    "reordered",
]


def create_spark_session():
    return (
        SparkSession.builder
        .appName("Process_Orders_Spark_To_ClickHouse")
        .master("local[*]")
        .config("spark.driver.memory", "1g")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def create_clickhouse_client():
    return Client(
        host=CLICKHOUSE_HOST,
        user=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
    )


def ensure_clickhouse_table(client):
    client.execute(f"CREATE DATABASE IF NOT EXISTS {DATABASE_NAME}")

    client.execute(f"""
        CREATE TABLE IF NOT EXISTS {DATABASE_NAME}.{TABLE_NAME} (
            batch_id String,
            ingested_at DateTime,

            order_id UInt64,
            user_id UInt64,
            order_number UInt32,
            order_dow UInt8,
            order_hour_of_day UInt8,
            days_since_prior_order Nullable(Float64),
            eval_set String,

            product_id UInt64,
            product_name String,
            aisle_id UInt32,
            aisle String,
            department_id UInt32,
            department String,
            add_to_cart_order UInt32,
            reordered UInt8
        )
        ENGINE = MergeTree()
        ORDER BY (batch_id, order_id, add_to_cart_order)
    """)


def clean_and_cast(df):
    cleaned_df = (
        df
        .filter(F.col("batch_id").isNotNull())
        .filter(F.col("order_id").isNotNull())
        .filter(F.col("product_id").isNotNull())
        .filter(F.col("add_to_cart_order").isNotNull())
        .select(
            F.col("batch_id").cast("string").alias("batch_id"),
            F.col("ingested_at").cast("timestamp").alias("ingested_at"),

            F.col("order_id").cast("long").alias("order_id"),
            F.col("user_id").cast("long").alias("user_id"),
            F.col("order_number").cast("int").alias("order_number"),
            F.col("order_dow").cast("int").alias("order_dow"),
            F.col("order_hour_of_day").cast("int").alias("order_hour_of_day"),
            F.col("days_since_prior_order").cast("double").alias("days_since_prior_order"),
            F.coalesce(F.col("eval_set").cast("string"), F.lit("")).alias("eval_set"),

            F.col("product_id").cast("long").alias("product_id"),
            F.coalesce(F.col("product_name").cast("string"), F.lit("")).alias("product_name"),
            F.col("aisle_id").cast("int").alias("aisle_id"),
            F.coalesce(F.col("aisle").cast("string"), F.lit("")).alias("aisle"),
            F.col("department_id").cast("int").alias("department_id"),
            F.coalesce(F.col("department").cast("string"), F.lit("")).alias("department"),
            F.col("add_to_cart_order").cast("int").alias("add_to_cart_order"),
            F.col("reordered").cast("int").alias("reordered"),
        )
    )

    return cleaned_df


def dataframe_to_tuples(df, columns):
    for row in df.toLocalIterator():
        yield tuple(row[column] for column in columns)


def cleanup_parquet_files():
    parquet_paths = glob.glob(f"{DATA_LAKE_PATH}/*.parquet")

    print("Cleaning processed Parquet files...")

    for path in parquet_paths:
        try:
            if os.path.isdir(path):
                shutil.rmtree(path)
            else:
                os.remove(path)
            print(f"Deleted: {path}")
        except OSError as error:
            print(f"Failed to delete {path}: {error}")


def process_orders():
    parquet_paths = glob.glob(f"{DATA_LAKE_PATH}/*.parquet")

    if not parquet_paths:
        raise FileNotFoundError(
            f"No Parquet files found in {DATA_LAKE_PATH}. Run Phase 2 first."
        )

    spark = create_spark_session()

    try:
        print("Reading Parquet files from Data Lake...")
        raw_df = spark.read.parquet(f"{DATA_LAKE_PATH}/*.parquet")

        print("Raw schema:")
        raw_df.printSchema()

        latest_batch_id = (
            raw_df
            .select("batch_id")
            .where(F.col("batch_id").isNotNull())
            .distinct()
            .orderBy(F.desc("batch_id"))
            .limit(1)
            .collect()[0]["batch_id"]
        )

        print(f"Latest batch_id found: {latest_batch_id}")

        latest_batch_df = raw_df.filter(F.col("batch_id") == latest_batch_id)

        print("Cleaning and enforcing schema using Spark...")
        final_df = clean_and_cast(latest_batch_df)

        print("Final schema:")
        final_df.printSchema()

        print("Sample cleaned rows:")
        final_df.show(10, truncate=False)

        row_count = final_df.count()

        if row_count == 0:
            raise ValueError("Final DataFrame has 0 rows after cleaning.")

        print(f"Rows ready for ClickHouse: {row_count}")

        print("Connecting to ClickHouse...")
        client = create_clickhouse_client()

        print("Creating database and table if needed...")
        ensure_clickhouse_table(client)

        print("Truncating old data...")
        client.execute(f"TRUNCATE TABLE {DATABASE_NAME}.{TABLE_NAME}")

        print("Inserting new rows into ClickHouse...")
        insert_query = f"""
            INSERT INTO {DATABASE_NAME}.{TABLE_NAME}
            ({", ".join(ORDER_ITEMS_COLUMNS)})
            VALUES
        """

        data_tuples = list(dataframe_to_tuples(final_df, ORDER_ITEMS_COLUMNS))

        client.execute(insert_query, data_tuples)

        print(f"Inserted {len(data_tuples)} rows into {DATABASE_NAME}.{TABLE_NAME}")

        cleanup_parquet_files()

        print("Phase 3 completed successfully.")

    finally:
        spark.stop()


if __name__ == "__main__":
    process_orders()