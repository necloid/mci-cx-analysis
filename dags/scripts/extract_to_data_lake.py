import os
import sys
import logging
import zipfile

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

DATA_LAKE_DIR = "/opt/airflow/data_lake"

def extract_latest_archive():
    logger.info("🔍 Scanning data lake for .zip files...")
    
    if not os.path.exists(DATA_LAKE_DIR):
        logger.error(f"❌ Directory {DATA_LAKE_DIR} was not found.")
        sys.exit(1)
        
    zip_files = [
        f for f in os.listdir(DATA_LAKE_DIR) 
        if f.lower().startswith("funnel_") and f.lower().endswith(".zip")
    ]
    
    if not zip_files:
        logger.error("❌ No 'Funnel_*.zip' archive files were found to extract.")
        sys.exit(1)
        
    # choose the newest one alphabetically/timestamp-wise
    target_zip = sorted(zip_files, reverse=True)[0]
    zip_path = os.path.join(DATA_LAKE_DIR, target_zip)
    
    logger.info(f"📦 Selected extraction target: {target_zip}")
    
    try:
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(DATA_LAKE_DIR)
        logger.info(f"✅ Extraction successful! All CSV files are available in {DATA_LAKE_DIR}")
        os.remove(zip_path)
        logger.info(f"🧹 Archive file {target_zip} has been removed to conserve storage space.")
        
    except zipfile.BadZipFile:
        logger.error(f"❌ File {target_zip} is not a valid or is a corrupted ZIP archive.")
        if os.path.exists(zip_path):
            os.remove(zip_path)
        sys.exit(1)
    except Exception as e:
        logger.error(f"❌ Extraction failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    extract_latest_archive()