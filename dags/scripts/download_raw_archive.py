import os
import sys
import logging
import re
import requests
import gdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)

FOLDER_ID = "1jy4qxnJdlkQG_xwzU4RFqso-Xl9dVIB1"
DESTINATION_DIR = "/opt/airflow/data_lake"

def download_dataset():
    os.makedirs(DESTINATION_DIR, exist_ok=True)

    folder_url = f"https://drive.google.com/embeddedfolderview?id={FOLDER_ID}"

    try:
        response = requests.get(folder_url, timeout=30)
        response.raise_for_status()
    except requests.RequestException as e:
        logger.error(f"Failed to access Google Drive folder: {e}")
        sys.exit(1)

    entries = re.findall(
        r'id="entry-([^"]+)".*?<div class="flip-entry-title">([^<]+)</div>',
        response.text,
        re.DOTALL
    )

    files = [
        (file_id, file_name)
        for file_id, file_name in entries
        if re.match(r"^Funnel_.*\.zip$", file_name, re.IGNORECASE)
    ]

    if not files:
        logger.error("No matching Funnel_*.zip files found.")
        sys.exit(1)

    # pick latest file alphabetically
    file_id, file_name = sorted(files, key=lambda x: x[1], reverse=True)[0]
    destination_path = os.path.join(DESTINATION_DIR, file_name)
   
    logger.info(f"Downloading {file_name}...")

    try:
        gdown.download(
            f"https://drive.google.com/uc?id={file_id}",
            destination_path,
            quiet=False
        )
        if not os.path.exists(destination_path) or os.path.getsize(destination_path) == 0:
            raise Exception("Downloaded file is empty.")
        logger.info(f"Download completed: {destination_path}")

    except Exception as e:
        logger.error(f"Download failed: {e}")
        if os.path.exists(destination_path):
            os.remove(destination_path)
        sys.exit(1)

if __name__ == "__main__":
    download_dataset()