import os
import sys
import logging
import clickhouse_connect

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", 8123))
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD")

def run_silver_transformation_pipeline():
    logger.info("🔌 Initializing connection to ClickHouse OLAP Cluster...")
    try:
        client = clickhouse_connect.get_client(
            host=CLICKHOUSE_HOST,
            port=CLICKHOUSE_PORT,
            username=CLICKHOUSE_USER,
            password=CLICKHOUSE_PASSWORD
        )
    except Exception as e:
        logger.error(f"❌ Failed to connect to ClickHouse Server: {e}")
        sys.exit(1)

    logger.info("🛠️  Creating core database namespace if not exists...")
    client.command("CREATE DATABASE IF NOT EXISTS core;")

    SILVER_TABLES_DDL = {
        "core.customers": """
            CREATE TABLE IF NOT EXISTS core.customers (
                customer_id String,
                customer_unique_id String,
                customer_zip_code_prefix String,
                customer_city String,
                customer_state LowCardinality(String)
            ) ENGINE = MergeTree() ORDER BY customer_id;
        """,
        "core.orders": """
            CREATE TABLE IF NOT EXISTS core.orders (
                order_id String,
                customer_id String,
                order_status LowCardinality(String),
                order_purchase_timestamp DateTime,
                order_approved_at Nullable(DateTime),
                order_delivered_carrier_date Nullable(DateTime),
                order_delivered_customer_date Nullable(DateTime),
                order_estimated_delivery_date DateTime
            ) ENGINE = MergeTree() ORDER BY order_purchase_timestamp;
        """,
        "core.order_items": """
            CREATE TABLE IF NOT EXISTS core.order_items (
                order_id String,
                order_item_id UInt8,
                product_id String,
                seller_id String,
                shipping_limit_date DateTime,
                price Float64,
                freight_value Float64
            ) ENGINE = MergeTree() ORDER BY order_id;
        """,
        "core.order_reviews": """
            CREATE TABLE IF NOT EXISTS core.order_reviews (
                review_id String,
                order_id String,
                review_score UInt8,
                review_comment_title Nullable(String),
                review_comment_message Nullable(String),
                review_creation_date DateTime,
                review_answer_timestamp DateTime
            ) ENGINE = MergeTree() ORDER BY (order_id, review_id);
        """,
        "core.order_payments": """
            CREATE TABLE IF NOT EXISTS core.order_payments (
                order_id String,
                payment_sequential UInt8,
                payment_type LowCardinality(String),
                payment_installments UInt8,
                payment_value Float64
            ) ENGINE = MergeTree() ORDER BY order_id;
        """,
        "core.products": """
            CREATE TABLE IF NOT EXISTS core.products (
                product_id String,
                product_category_name Nullable(String),
                product_name_lenght Nullable(UInt16),
                product_description_lenght Nullable(UInt16),
                product_photos_qty Nullable(UInt8),
                product_weight_g Nullable(UInt32),
                product_length_cm Nullable(UInt8),
                product_height_cm Nullable(UInt8),
                product_width_cm Nullable(UInt8)
            ) ENGINE = MergeTree() ORDER BY product_id;
        """,
        "core.sellers": """
            CREATE TABLE IF NOT EXISTS core.sellers (
                seller_id String,
                seller_zip_code_prefix String,
                seller_city String,
                seller_state LowCardinality(String)
            ) ENGINE = MergeTree() ORDER BY seller_id;
        """,
        "core.geolocation": """
            CREATE TABLE IF NOT EXISTS core.geolocation (
                geolocation_zip_code_prefix String,
                geolocation_lat Float64,
                geolocation_lng Float64,
                geolocation_city String,
                geolocation_state LowCardinality(String)
            ) ENGINE = MergeTree() ORDER BY geolocation_zip_code_prefix;
        """,
        "core.category_translation": """
            CREATE TABLE IF NOT EXISTS core.category_translation (
                product_category_name String,
                product_category_name_english String
            ) ENGINE = MergeTree() ORDER BY product_category_name;
        """,
        "core.mql": """
            CREATE TABLE IF NOT EXISTS core.mql (
                mql_id String,
                first_contact_date Nullable(DateTime),
                landing_page_id String,
                origin LowCardinality(Nullable(String))
            ) ENGINE = MergeTree() ORDER BY mql_id;
        """,
        "core.closed_deals": """
            CREATE TABLE IF NOT EXISTS core.closed_deals (
                mql_id String,
                seller_id String,
                sdr_id String,
                sr_id String,
                won_date Nullable(DateTime),
                business_segment Nullable(String),
                lead_type Nullable(String),
                business_type Nullable(String),
                declared_monthly_revenue Nullable(Float64)
            ) ENGINE = MergeTree() ORDER BY mql_id;
        """
    }

    # Query Transformasi Data dari Staging ke Core
    SILVER_TRANSFORM_QUERIES = {
        "core.customers": """
            INSERT INTO core.customers
            SELECT customer_id, customer_unique_id, customer_zip_code_prefix, customer_city, customer_state
            FROM staging.customers;
        """,
        "core.orders": """
            INSERT INTO core.orders
            SELECT 
                order_id,
                customer_id,
                order_status,
                parseDateTimeBestEffort(order_purchase_timestamp) AS order_purchase_timestamp,
                order_approved_at = '' ? NULL : parseDateTimeBestEffort(order_approved_at) AS order_approved_at,
                order_delivered_carrier_date = '' ? NULL : parseDateTimeBestEffort(order_delivered_carrier_date) AS order_delivered_carrier_date,
                order_delivered_customer_date = '' ? NULL : parseDateTimeBestEffort(order_delivered_customer_date) AS order_delivered_customer_date,
                parseDateTimeBestEffort(order_estimated_delivery_date) AS order_estimated_delivery_date
            FROM staging.orders;
        """,
        "core.order_items": """
            INSERT INTO core.order_items
            SELECT 
                order_id,
                toUInt8(order_item_id) AS order_item_id,
                product_id,
                seller_id,
                parseDateTimeBestEffort(shipping_limit_date) AS shipping_limit_date,
                toFloat64(price) AS price,
                toFloat64(freight_value) AS freight_value
            FROM staging.order_items;
        """,
        "core.order_reviews": """
            INSERT INTO core.order_reviews
            SELECT 
                review_id,
                order_id,
                toUInt8(review_score) AS review_score,
                review_comment_title = '' ? NULL : review_comment_title AS review_comment_title,
                review_comment_message = '' ? NULL : review_comment_message AS review_comment_message,
                parseDateTimeBestEffort(review_creation_date) AS review_creation_date,
                parseDateTimeBestEffort(review_answer_timestamp) AS review_answer_timestamp
            FROM staging.order_reviews;
        """,
        "core.order_payments": """
            INSERT INTO core.order_payments
            SELECT 
                order_id,
                toUInt8(payment_sequential) AS payment_sequential,
                payment_type,
                toUInt8(payment_installments) AS payment_installments,
                toFloat64(payment_value) AS payment_value
            FROM staging.order_payments;
        """,
        "core.products": """
            INSERT INTO core.products
            SELECT 
                product_id,
                product_category_name = '' ? NULL : product_category_name AS product_category_name,
                product_name_lenght = '' ? NULL : toUInt16(product_name_lenght) AS product_name_lenght,
                product_description_lenght = '' ? NULL : toUInt16(product_description_lenght) AS product_description_lenght,
                product_photos_qty = '' ? NULL : toUInt8(product_photos_qty) AS product_photos_qty,
                product_weight_g = '' ? NULL : toUInt32(product_weight_g) AS product_weight_g,
                product_length_cm = '' ? NULL : toUInt8(product_length_cm) AS product_length_cm,
                product_height_cm = '' ? NULL : toUInt8(product_height_cm) AS product_height_cm,
                product_width_cm = '' ? NULL : toUInt8(product_width_cm) AS product_width_cm
            FROM staging.products;
        """,
        "core.sellers": """
            INSERT INTO core.sellers
            SELECT seller_id, seller_zip_code_prefix, seller_city, seller_state
            FROM staging.sellers;
        """,
        "core.geolocation": """
            INSERT INTO core.geolocation
            SELECT 
                geolocation_zip_code_prefix,
                toFloat64(geolocation_lat) AS geolocation_lat,
                toFloat64(geolocation_lng) AS geolocation_lng,
                geolocation_city,
                geolocation_state
            FROM staging.geolocation;
        """,
        "core.category_translation": """
            INSERT INTO core.category_translation
            SELECT product_category_name, product_category_name_english
            FROM staging.category_translation;
        """,
        "core.mql": """
            INSERT INTO core.mql
            SELECT 
                mql_id,
                first_contact_date = '' ? NULL : parseDateTimeBestEffort(first_contact_date) AS first_contact_date,
                landing_page_id,
                origin = '' ? NULL : origin AS origin
            FROM staging.mql;
        """,
        "core.closed_deals": """
            INSERT INTO core.closed_deals
            SELECT 
                mql_id,
                seller_id,
                sdr_id,
                sr_id,
                won_date = '' ? NULL : parseDateTimeBestEffort(won_date) AS won_date,
                business_segment = '' ? NULL : business_segment AS business_segment,
                lead_type = '' ? NULL : lead_type AS lead_type,
                business_type = '' ? NULL : business_type AS business_type,
                declared_monthly_revenue = '' ? NULL : toFloat64(declared_monthly_revenue) AS declared_monthly_revenue
            FROM staging.closed_deals;
        """
    }

    # Loop untuk mengeksekusi migrasi data per tabel
    total_tables = len(SILVER_TABLES_DDL)
    logger.info(f"🚀 Found {total_tables} target core models to compile. Processing data...")

    for idx, (table_name, ddl_query) in enumerate(SILVER_TABLES_DDL.items(), 1):
        logger.info(f" [{idx}/{total_tables}] Compiling architecture for: {table_name}")
        
        if table_name == "core.order_reviews":
            client.command(f"DROP TABLE IF EXISTS {table_name};")
            
        client.command(ddl_query)

        logger.info(f" 🧹 Wiping target records in {table_name} to guarantee idempotency...")
        client.command(f"TRUNCATE TABLE {table_name};")

        logger.info(f" ✨ Ingesting high-performance typed records into {table_name}...")
        try:
            client.command(SILVER_TRANSFORM_QUERIES[table_name])
            
            row_count = client.command(f"SELECT count() FROM {table_name}")
            logger.info(f" ✅ Materialization success! {table_name} holds {row_count} clean rows.")
        except Exception as e:
            logger.error(f" ❌ Critical error executing Core Transformation on {table_name}: {e}")
            sys.exit(1)

    logger.info("🎉 SUCCESS: Core Layer has been successfully deployed!")

if __name__ == "__main__":
    run_silver_transformation_pipeline()