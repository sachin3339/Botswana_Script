import json
import logging
import os

from google.cloud import storage

logger = logging.getLogger(__name__)


def upload_to_bucket(blob_name: str, path_to_file: str, bucket_name: str, cred_file_path: str):
    ''' Uploads given file to given google cloud bucket. '''

    storage_client = storage.Client.from_service_account_json(cred_file_path)
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.upload_from_filename(path_to_file)
    return blob.public_url


def upload_csv(date_str: str, config: dict):
    path_name = os.path.join(config['download_dir'], date_str, date_str)

    # To upload all 3 files to same directory
    upload_to_bucket(f'consolidated_data/{date_str}_consolidated_data.csv',
                        f'{path_name}_consolidated_data.csv', config['bucket_name'], config['google_cloud_cred'])
    logger.info(f"Completed Uploading Consolidated Data for {date_str}")
    upload_to_bucket(f'department_summary/{date_str}_department_summary.csv',
                        f'{path_name}_department_summary.csv', config['bucket_name'], config['google_cloud_cred'])
    logger.info(f"Completed Uploading Department Summary for {date_str}")
    upload_to_bucket(f'executive_summary/{date_str}_executive_summary.csv',
                        f'{path_name}_executive_summary.csv', config['bucket_name'], config['google_cloud_cred'])
    logger.info(f"Completed Uploading Executive Summary for {date_str}")

    # To upload latest consolidated data to another directory
    upload_to_bucket('current_consolidated_data/consolidated_data.csv',
                        f'{path_name}_consolidated_data.csv', config['bucket_name'], config['google_cloud_cred'])
    logger.info(f"Completed Uploading Current Consolidated for {date_str}")

    # To upload delta_data csv file
    if os.path.exists(f"{path_name}_delta_data.csv"):
        upload_to_bucket(f'delta_data/{date_str}_delta_data.csv',
                            f"{path_name}_delta_data.csv", config['bucket_name'], config['google_cloud_cred'])
        logger.info(f"Completed Uploading Delta for {date_str}")
    else:
        logger.info(
            "Delta data file not uploaded cause it's not present in required folder.")


def upload_all_files(date_str):
    # Reading config file
    with open('config.json') as f:
        config = json.load(f)

    # To upload to Google Cloud Storage
    logger.info("Starting upload of csv files.")
    upload_csv(date_str, config)
    logger.info("Done uploading csv files.")
    logger.info("All files successfully uploaded!")
