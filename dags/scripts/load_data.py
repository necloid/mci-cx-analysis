import os
import sys
import logging
import pandas as pd
import clickhouse_connect

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

DATA_LAKE_DIR = "/opt/airflow/data_lake"

# clickhouse connection config
CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "clickhouse-server")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", 8123))
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "admin")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "rahasia")

# DDL for staging tables
STAGING_TABLES_DDL = {
    "staging_category_translation": """
        CREATE TABLE IF NOT EXISTS staging.category_translation (
            product_category_name String,
            product_category_name_english String
        ) ENGINE = MergeTree() ORDER BY tuple();
    """,
    "staging_customers": """
        CREATE TABLE IF NOT EXISTS staging.customers (
            customer_id String,
            customer_unique_id String,
            customer_zip_code_prefix String,
            customer_city String,
            customer_state String
        ) ENGINE = MergeTree() ORDER BY tuple();
    """,
    "staging_orders": """
        CREATE TABLE IF NOT EXISTS staging.orders (
            order_id String,
            customer_id String,
            order_status String,
            order_purchase_timestamp Nullable(String),
            order_approved_at Nullable(String),
            order_delivered_carrier_date Nullable(String),
            order_delivered_customer_date Nullable(String),
            order_estimated_delivery_date Nullable(String)
        ) ENGINE = MergeTree() ORDER BY tuple();
    """,
    "staging_order_items": """
        CREATE TABLE IF NOT EXISTS staging.order_items (
            order_id String,
            order_item_id String,
            product_id String,
            seller_id String,
            shipping_limit_date Nullable(String),
            price Nullable(String),
            freight_value Nullable(String)
        ) ENGINE = MergeTree() ORDER BY tuple();
    """,
    "staging_order_payments": """
        CREATE TABLE IF NOT EXISTS staging.order_payments (
            order_id String,
            payment_sequential String,
            payment_type String,
            payment_installments String,
            payment_value Nullable(String)
        ) ENGINE = MergeTree() ORDER BY tuple();
    """,
    "staging_order_reviews": """
        CREATE TABLE IF NOT EXISTS staging.order_reviews (
            review_id String,
            order_id String,
            review_score String,
            review_comment_title Nullable(String),
            review_comment_message Nullable(String),
            review_creation_date Nullable(String),
            review_answer_timestamp Nullable(String)
        ) ENGINE = MergeTree() ORDER BY tuple();
    """,
    "staging_products": """
        CREATE TABLE IF NOT EXISTS staging.products (
            product_id String,
            product_category_name Nullable(String),
            product_name_lenght Nullable(String),
            product_description_lenght Nullable(String),
            product_photos_qty Nullable(String),
            product_weight_g Nullable(String),
            product_length_cm Nullable(String),
            product_height_cm Nullable(String),
            product_width_cm Nullable(String)
        ) ENGINE = MergeTree() ORDER BY tuple();
    """,
    "staging_sellers": """
        CREATE TABLE IF NOT EXISTS staging.sellers (
            seller_id String,
            seller_zip_code_prefix String,
            seller_city String,
            seller_state String
        ) ENGINE = MergeTree() ORDER BY tuple();
    """,
    "staging_geolocation": """
        CREATE TABLE IF NOT EXISTS staging.geolocation (
            geolocation_zip_code_prefix String,
            geolocation_lat String,
            geolocation_lng String,
            geolocation_city String,
            geolocation_state String
        ) ENGINE = MergeTree() ORDER BY tuple();
    """,
    "staging_mql": """
        CREATE TABLE IF NOT EXISTS staging.mql (
            mql_id String,
            first_contact_date Nullable(String),
            landing_page_id String,
            origin Nullable(String)
        ) ENGINE = MergeTree() ORDER BY tuple();
    """,
    "staging_closed_deals": """
        CREATE TABLE IF NOT EXISTS staging.closed_deals (
            mql_id String,
            seller_id String,
            sdr_id String,
            sr_id String,
            won_date Nullable(String),
            business_segment Nullable(String),
            lead_type Nullable(String),
            lead_behaviour_profile Nullable(String),
            has_company Nullable(String),
            has_gtin Nullable(String),
            average_stock Nullable(String),
            business_type Nullable(String),
            declared_product_catalog_size Nullable(String),
            declared_monthly_revenue Nullable(String)
        ) ENGINE = MergeTree() ORDER BY tuple();
    """
}

# mapping of local CSV file names to staging table names
CSV_TO_TABLE_MAPPING = {
    "category_translation.csv": "category_translation",
    "customers.csv": "customers",
    "orders.csv": "orders",
    "order_items.csv": "order_items",
    "order_payments.csv": "order_payments",
    "order_reviews.csv": "order_reviews",
    "products.csv": "products",
    "sellers.csv": "sellers",
    "geolocation.csv": "geolocation",
    "mql.csv": "mql",
    "closed_deals.csv": "closed_deals"
}

def load_staging_layer():
    logger.info("🔌 Connecting to the ClickHouse database cluster...")
    try:
        client = clickhouse_connect.get_client(
            host=CLICKHOUSE_HOST,
            port=CLICKHOUSE_PORT,
            username=CLICKHOUSE_USER,
            password=CLICKHOUSE_PASSWORD
        )
    except Exception as e:
        logger.error(f"❌ Failed to connect to ClickHouse: {e}")
        sys.exit(1)

    logger.info("🛠️ Preparing the 'staging' database namespace...")
    client.command("CREATE DATABASE IF NOT EXISTS staging;")

    # iterate to create table structures and load CSV data
    for csv_file, table_name in CSV_TO_TABLE_MAPPING.items():
        csv_path = os.path.join(DATA_LAKE_DIR, csv_file)
        full_table_name = f"staging.{table_name}"
        
        if not os.path.exists(csv_path):
            logger.warning(f"⚠️ Data file {csv_file} was not found in {DATA_LAKE_DIR}. Skipping this table.")
            continue

        # create the table DDL for this specific table if needed
        logger.info(f"🏗️ Creating table (if does not exist): {full_table_name}")
        client.command(STAGING_TABLES_DDL[f"staging_{table_name}"])

        # truncate old data to make each run idempotent
        logger.info(f"🧹 Clearing old data in {full_table_name}...")
        client.command(f"TRUNCATE TABLE {full_table_name};")

        # load CSV into df and insert into clickhouse
        logger.info(f"📥 Loading data from {csv_file} into ClickHouse...")
        try:
            df = pd.read_csv(csv_path, dtype=str, keep_default_na=False)
            client.insert_df(table=table_name, df=df, database="staging")

            # post-ingestion integrity audit: count the number of rows loaded
            row_count = client.command(f"SELECT count() FROM {full_table_name}")
            logger.info(f"✅ Successfully loaded {full_table_name}! Total records: {row_count} rows.")

        except Exception as e:
            logger.error(f"❌ Failed to load file {csv_file} into {full_table_name}: {e}")
            sys.exit(1)

    logger.info("🏁 Finished! Staging layer populated and ready for the clean layer.")

if __name__ == "__main__":
    load_staging_layer()