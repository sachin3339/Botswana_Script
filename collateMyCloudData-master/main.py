''' Run this file to finally upload all files to Google Cloud Storage bucket. '''

import logging
from datetime import datetime

from collate_data import main as collate_main
from download_excel_files import main as download_main
from process_delta import main as delta_main
from upload_files import upload_all_files
from verify_download import main as verify_main

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


if __name__ == '__main__':
    
    # Settings date string
    date_str = datetime.today().strftime("%Y-%m-%d")
    
    # Downloading excel files
    download_main(date_str)

    # Verifying downloaded files
    verify_main(date_str)
    
    # Creating csv files and uploading it
    collate_main(date_str)

    # Processing collate data 
    delta_main(date_str)

    # Uploading all files to Google Cloud Storage
    upload_all_files(date_str)
