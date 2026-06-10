import os
import sys
import logging
import clickhouse_connect

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "clickhouse-server")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", 8123))
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "admin")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "rahasia")

def run_analytics_aggregation_pipeline():
    logger.info("🔌 Connecting to ClickHouse OLAP Cluster for Analytics Layer...")
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

    logger.info("🛠️  Preparing the analytics database namespace...")
    client.command("CREATE DATABASE IF NOT EXISTS analytics;")

    DATA_MARTS_QUERIES = {
        # MART 1: Customer Experience Sentiment Analytics Mart
        "analytics.mart_customer_experience_sentiment": """
            CREATE TABLE analytics.mart_customer_experience_sentiment 
            ENGINE = MergeTree() 
            ORDER BY review_id AS
            SELECT 
                r.review_id,
                r.order_id,
                r.review_score,
                r.review_comment_message,
                length(r.review_comment_message) AS comment_length,
                
                -- Sentiment Lexicon Engine (Portuguese Rules + Score Fallback)
                CASE 
                    WHEN multiSearchAny(lower(r.review_comment_message), ['atraso', 'demorou', 'não recebi', 'nao recebi', 'não veio', 'nao veio', 'faltando', 'faltou', 'incompleto', 'quantidade', 'ruim', 'pessimo', 'quebrado', 'estragado', 'defeito', 'falta', 'errado']) THEN 'Negative'
                    WHEN multiSearchAny(lower(r.review_comment_message), ['excelente', 'otimo', 'perfeito', 'lindo', 'adorou', 'parabens', 'recomendo', 'rápido', 'bom', '100%']) THEN 'Positive'
                    ELSE 
                        CASE 
                            WHEN r.review_score <= 2 THEN 'Negative'
                            WHEN r.review_score >= 4 THEN 'Positive'
                            ELSE 'Neutral'
                        END
                END AS sentiment_category,

                -- Feedback Root-Cause Extraction Engine
                CASE 
                    WHEN sentiment_category = 'Negative' THEN
                        CASE 
                            WHEN multiSearchAny(lower(r.review_comment_message), ['não recebi', 'nao recebi', 'não veio', 'nao veio', 'faltando', 'faltou', 'incompleto', 'quantidade', 'apenas um', 'somente um', 'menos do que', 'menos que', 'produto a menos']) THEN 'Warehouse (Missing Items / Quantity Shortage)'
                            WHEN multiSearchAny(lower(r.review_comment_message), ['atraso', 'demorou', 'prazo', 'entreg']) THEN 'Logistics (Delay)'
                            WHEN multiSearchAny(lower(r.review_comment_message), ['quebrado', 'estragado', 'defeito', 'danificado', 'pessima qualidade']) THEN 'Product Quality (Defect)'
                            WHEN multiSearchAny(lower(r.review_comment_message), ['errado', 'trocado', 'falta', 'incompleto']) THEN 'Warehouse (Wrong Delivery)'
                            ELSE 'Other Complaints (Rating-based)'
                        END
                    WHEN sentiment_category = 'Positive' THEN
                        CASE 
                            WHEN multiSearchAny(lower(r.review_comment_message), ['rápido', 'antes do prazo', 'velocidade', 'vapt']) THEN 'Praise: Fast Delivery'
                            WHEN multiSearchAny(lower(r.review_comment_message), ['excelente', 'otimo', 'perfeito', 'lindo', 'maravilhoso', 'qualidade']) THEN 'Praise: Product Quality'
                            ELSE 'General Appreciation'
                        END
                    ELSE 'Neutral / No Comment'
                END AS customer_feedback_topic,

                c.customer_city,
                c.customer_state,
                oc.category_english
            FROM core.order_reviews r
            LEFT JOIN core.orders o ON r.order_id = o.order_id
            LEFT JOIN core.customers c ON o.customer_id = c.customer_id
            LEFT JOIN (
                SELECT
                    oi.order_id AS order_id,
                    argMin(t.product_category_name_english, oi.order_item_id) AS category_english
                FROM core.order_items oi
                LEFT JOIN core.products p ON oi.product_id = p.product_id
                LEFT JOIN core.category_translation t ON p.product_category_name = t.product_category_name
                GROUP BY oi.order_id
            ) oc ON o.order_id = oc.order_id;
        """,

        # MART 2: Delivery & SLA Fulfillment Lags Impact Mart
        "analytics.mart_cx_delivery_impact": """
            CREATE TABLE analytics.mart_cx_delivery_impact 
            ENGINE = MergeTree() 
            ORDER BY is_late AS
            SELECT 
                o.order_id,
                o.customer_id,
                r.review_score,
                o.order_purchase_timestamp AS purchase_date,
                o.order_estimated_delivery_date AS estimated_delivery_date,
                o.order_delivered_customer_date AS actual_delivery_date,
                
                -- check if delivery is late
                if(isNotNull(o.order_delivered_customer_date) AND (o.order_delivered_customer_date > o.order_estimated_delivery_date), 1, 0) AS is_late,
                
                -- total delay in days (actual - estimated)
                if(isNull(o.order_delivered_customer_date), 0, dateDiff('day', o.order_estimated_delivery_date, o.order_delivered_customer_date)) AS days_delay,
                
                -- seller handling time: purchase to carrier (in days)
                if(isNotNull(o.order_delivered_carrier_date) AND isNotNull(o.order_purchase_timestamp), dateDiff('day', o.order_purchase_timestamp, o.order_delivered_carrier_date), 0) AS seller_handling_days,
                
                -- carrier transit time: carrier to customer (in days)
                if(isNotNull(o.order_delivered_customer_date) AND isNotNull(o.order_delivered_carrier_date), dateDiff('day', o.order_delivered_carrier_date, o.order_delivered_customer_date), 0) AS carrier_transit_days,
                
                -- freight cost ratio percent
                oi.freight_value_pct
            FROM core.orders o
            LEFT JOIN core.order_reviews r ON o.order_id = r.order_id
            LEFT JOIN (
                SELECT 
                    order_id, 
                    round(sum(freight_value) / (sum(price) + 0.0001) * 100, 2) AS freight_value_pct
                FROM core.order_items
                GROUP BY order_id
            ) oi ON o.order_id = oi.order_id
            WHERE o.order_status = 'delivered';
        """,

        # MART 3: Product Attributes & Seller Quality Insights Mart
        "analytics.mart_cx_product_and_seller_quality": """
            CREATE TABLE analytics.mart_cx_product_and_seller_quality 
            ENGINE = MergeTree() 
            ORDER BY (seller_id, order_id, order_item_id) AS
            SELECT 
                oi.order_item_id,
                oi.order_id,
                oi.product_id,
                oi.seller_id,
                r.review_score,
                t.product_category_name_english AS product_category,
                p.product_photos_qty AS product_photos_qty,
                p.product_weight_g AS product_weight_g,
                p.product_description_lenght AS product_description_length,
                
                -- seller aggregate metrics
                s_meta.total_seller_orders,
                s_meta.seller_bayesian_rating,
                s_meta.seller_late_shipping_rate
            FROM core.order_items oi
            LEFT JOIN core.order_reviews r ON oi.order_id = r.order_id
            LEFT JOIN core.products p ON oi.product_id = p.product_id
            LEFT JOIN core.category_translation t ON p.product_category_name = t.product_category_name
            LEFT JOIN (
                SELECT 
                    temp.seller_id AS seller_id,
                    count(DISTINCT temp.order_id) AS total_seller_orders,
                    
                    -- bayesian adjusted rating (m=15)
                    round(
                        ((count(DISTINCT temp.order_id) * avg(temp.review_score)) + (15 * (SELECT avg(r3.review_score) FROM core.order_reviews r3))) 
                        / (count(DISTINCT temp.order_id) + 15), 
                        2
                    ) AS seller_bayesian_rating,
                    
                    -- late shipping rate (shipping limit date exceeded)
                    round(
                        sum(temp.isLateShipping) / count() * 100, 
                        2
                    ) AS seller_late_shipping_rate
                FROM (
                    SELECT 
                        oi2.seller_id AS seller_id,
                        oi2.order_id AS order_id,
                        r2.review_score AS review_score,
                        if(isNotNull(o2.order_delivered_carrier_date) AND (o2.order_delivered_carrier_date > oi2.shipping_limit_date), 1, 0) AS isLateShipping
                    FROM core.order_items oi2
                    LEFT JOIN core.orders o2 ON oi2.order_id = o2.order_id
                    LEFT JOIN core.order_reviews r2 ON oi2.order_id = r2.order_id
                ) AS temp
                GROUP BY temp.seller_id
            ) s_meta ON oi.seller_id = s_meta.seller_id;
        """,

        # MART 4: Regional Logistics & Freight Cost Gaps Mart
        "analytics.mart_cx_regional_logistics_gaps": """
            CREATE TABLE analytics.mart_cx_regional_logistics_gaps 
            ENGINE = MergeTree() 
            ORDER BY avg_review_score AS
            SELECT 
                c.customer_state AS customer_state,
                c.customer_city AS customer_city,
                count(DISTINCT o.order_id) AS total_orders,
                round(avg(r.review_score), 2) AS avg_review_score,
                
                -- percentage of late orders
                round(
                    sum(if(isNotNull(o.order_delivered_customer_date) AND (o.order_delivered_customer_date > o.order_estimated_delivery_date), 1, 0)) 
                    / count(DISTINCT o.order_id) * 100, 
                    2
                ) AS pct_orders_late,
                
                -- average freight value
                round(avg(oi.freight_value), 2) AS avg_freight_value,
                
                -- percentage of interstate orders
                round(
                    sum(if(c.customer_state != s.seller_state, 1, 0)) / count() * 100, 
                    2
                ) AS pct_interstate_shipments
            FROM core.orders o
            LEFT JOIN core.customers c ON o.customer_id = c.customer_id
            LEFT JOIN core.order_reviews r ON o.order_id = r.order_id
            LEFT JOIN core.order_items oi ON o.order_id = oi.order_id
            LEFT JOIN core.sellers s ON oi.seller_id = s.seller_id
            WHERE isNotNull(c.customer_state)
            GROUP BY customer_state, customer_city;
        """
    }

    total_marts = len(DATA_MARTS_QUERIES)
    failed_marts = []
    logger.info(f"🚀 Found {total_marts} analytical data marts to build. Executing materialization...")

    for idx, (mart_name, build_query) in enumerate(DATA_MARTS_QUERIES.items(), 1):
        logger.info(f" [{idx}/{total_marts}] Rebuilding Data Mart: {mart_name}")
        
        try:
            logger.info(f" Creating/refreshing target table: {mart_name}")
            client.command(f"DROP TABLE IF EXISTS {mart_name};")
            client.command(build_query)

            row_count = client.command(f"SELECT count() FROM {mart_name}")
            logger.info(f" Successfully materialized {mart_name}! Active rows: {row_count}")
        except Exception as e:
            logger.exception(f" Failed to compile analytical mart {mart_name}: {e}")
            failed_marts.append(mart_name)
            continue

    if failed_marts:
        logger.error(f"Analytics mart build finished with failures: {', '.join(failed_marts)}")
        sys.exit(1)

    logger.info("SUCCESS: All Analytics Marts are successfully loaded!")

if __name__ == "__main__":
    run_analytics_aggregation_pipeline()