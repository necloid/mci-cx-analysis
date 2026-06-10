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

CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", 8123))
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD")

def run_gold_aggregation_pipeline():
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

    # Data Mart Materialized Tables via Deep Joins & Sentiment Engines
    GOLD_MARTS_QUERIES = {
        # MART 1: Customer Experience Sentiment Analytics Mart (The Core Focus)
        "analytics.mart_customer_experience_sentiment": """
            CREATE TABLE analytics.mart_customer_experience_sentiment 
            ENGINE = MergeTree() 
            ORDER BY (sentiment_category, review_score) AS
            SELECT 
                r.review_id,
                r.order_id,
                r.review_score,
                r.review_comment_message,
                
                -- Hibrida Sentiment Lexicon Engine (Portuguese Rules + Score Fallback)
                CASE 
                    WHEN multiSearchAny(lower(r.review_comment_message), ['atraso', 'demorou', 'não recebi', 'ruim', 'pessimo', 'quebrado', 'estragado', 'defeito', 'falta', 'errado']) THEN 'Negative'
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
                            WHEN multiSearchAny(lower(r.review_comment_message), ['atraso', 'demorou', 'não recebi', 'prazo', 'entreg']) THEN 'Logistik (Keterlambatan)'
                            WHEN multiSearchAny(lower(r.review_comment_message), ['quebrado', 'estragado', 'defeito', 'danificado', 'pessima qualidade']) THEN 'Kualitas Produk (Cacat)'
                            WHEN multiSearchAny(lower(r.review_comment_message), ['errado', 'trocado', 'falta', 'incompleto']) THEN 'Gudang (Salah Kirim)'
                            ELSE 'Keluhan Lain-lain (Berbasis Rating)'
                        END
                    WHEN sentiment_category = 'Positive' THEN
                        CASE 
                            WHEN multiSearchAny(lower(r.review_comment_message), ['rápido', 'antes do prazo', 'velocidade', 'vapt']) THEN 'Pujian: Pengiriman Cepat'
                            WHEN multiSearchAny(lower(r.review_comment_message), ['excelente', 'otimo', 'perfeito', 'lindo', 'maravilhoso', 'qualidade']) THEN 'Pujian: Kualitas Produk'
                            ELSE 'Apresiasi Umum'
                        END
                    ELSE 'Netral / Tanpa Komentar'
                END AS customer_feedback_topic,

                c.customer_city,
                c.customer_state,
                t.product_category_name_english AS category_english
            FROM core.order_reviews r
            LEFT JOIN core.orders o ON r.order_id = o.order_id
            LEFT JOIN core.customers c ON o.customer_id = c.customer_id
            LEFT JOIN core.order_items oi ON o.order_id = oi.order_id
            LEFT JOIN core.products p ON oi.product_id = p.product_id
            LEFT JOIN core.category_translation t ON p.product_category_name = t.product_category_name;
        """,

        # MART 2: Delivery Logistics & SLA Compliance Performance Mart (FIXED)
        "analytics.mart_delivery_performance_impact": """
            CREATE TABLE analytics.mart_delivery_performance_impact 
            ENGINE = MergeTree() 
            ORDER BY is_late AS
            SELECT 
                o.order_id,
                o.customer_id,
                o.order_purchase_timestamp,
                o.order_delivered_customer_date,
                o.order_estimated_delivery_date,
                
                -- Menghitung durasi riil pengiriman (dalam satuan Hari)
                isNull(o.order_delivered_customer_date) ? 0 : dateDiff('day', o.order_purchase_timestamp, o.order_delivered_customer_date) AS delivery_duration_days,
                
                -- Status Pelanggaran SLA (Menggunakan isNotNull yang valid untuk DateTime)
                (isNotNull(o.order_delivered_customer_date) AND (o.order_delivered_customer_date > o.order_estimated_delivery_date)) ? 1 : 0 AS is_late,
                
                -- Selisih hari dari estimasi
                isNull(o.order_delivered_customer_date) ? 0 : dateDiff('day', o.order_estimated_delivery_date, o.order_delivered_customer_date) AS days_difference,
                
                r.review_score
            FROM core.orders o
            LEFT JOIN core.order_reviews r ON o.order_id = r.order_id
            WHERE o.order_status = 'delivered';
        """,

        # MART 3: Seller Compliance, Performance, & B2B Acquisition Channels Mart (BAYESIAN ADJUSTED)
        "analytics.mart_seller_compliance_and_acquisition": """
            CREATE TABLE analytics.mart_seller_compliance_and_acquisition 
            ENGINE = MergeTree() 
            ORDER BY bayesian_adjusted_rating AS
            SELECT 
                s.seller_id AS seller_id,
                m.origin AS seller_acquisition_channel,
                cd.business_segment AS seller_business_segment,
                count(DISTINCT oi.order_id) AS total_orders_handled,
                round(avg(r.review_score), 2) AS rating_asli,
                
                -- Formula Bayesian Rating Dinamis (m = 15) untuk menghindari small numbers bias
                round(
                    ((count(DISTINCT oi.order_id) * avg(r.review_score)) + (15 * (SELECT avg(review_score) FROM core.order_reviews))) 
                    / (count(DISTINCT oi.order_id) + 15), 
                    2
                ) AS bayesian_adjusted_rating,
                
                round(sum(oi.price), 2) AS total_revenue
            FROM core.sellers s
            LEFT JOIN core.order_items oi ON s.seller_id = oi.seller_id
            LEFT JOIN core.order_reviews r ON oi.order_id = r.order_id
            LEFT JOIN core.closed_deals cd ON s.seller_id = cd.seller_id
            LEFT JOIN core.mql m ON cd.mql_id = m.mql_id
            GROUP BY s.seller_id, seller_acquisition_channel, seller_business_segment;
        """
    }

    total_marts = len(GOLD_MARTS_QUERIES)
    logger.info(f"🚀 Found {total_marts} analytical data marts to build. Executing materialization...")

    for idx, (mart_name, build_query) in enumerate(GOLD_MARTS_QUERIES.items(), 1):
        logger.info(f" [{idx}/{total_marts}] Rebuilding Data Mart: {mart_name}")
        
        client.command(f"DROP TABLE IF EXISTS {mart_name};")
        
        try:
            # eksekusi kompilasi tabel analitik baru
            client.command(build_query)
            
            # Post-compilation data density report
            row_count = client.command(f"SELECT count() FROM {mart_name}")
            logger.info(f" ✅ Successfully materialized {mart_name}! Active rows: {row_count}")
        except Exception as e:
            logger.error(f" ❌ Failed to compile analytical mart {mart_name}: {e}")
            sys.exit(1)

    logger.info("🏁 SUCCESS: All Analytics Data Marts are successfully loaded!")

if __name__ == "__main__":
    run_gold_aggregation_pipeline()