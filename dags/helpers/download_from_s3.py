import os
import boto3
from dotenv import load_dotenv

from helpers.create_dir import create_dir
from helpers.constants import DOWNLOADED_FILES_FROM_S3

def download_file_from_s3(s3_key):
    """Downloads file from s3 by received s3 key.
    Credentials for AWS are taken from aws_resources"""
    load_dotenv()
    
    local_dir = os.path.join(os.getcwd(), DOWNLOADED_FILES_FROM_S3)
    create_dir(os.getcwd(), DOWNLOADED_FILES_FROM_S3)
    aws_access_key_id = os.getenv('ACCESS_KEY')
    aws_secret_access_key = os.getenv('SECRET_KEY')
    endpoint_url = os.getenv('ENDPOINT')
    
    s3_client = boto3.client(
        's3',
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        endpoint_url=endpoint_url
    )
    bucket_name = os.getenv('BUCKET_NAME')

    file_name = os.path.basename(s3_key)
    grouping_folder = os.path.basename(os.path.dirname(s3_key))
    target_path = os.path.join(local_dir, grouping_folder, file_name)
    create_dir(local_dir, grouping_folder)
    try:
        s3_client.download_file(bucket_name, s3_key, target_path)
        print(f'OK: File {s3_key} downloaded successfully to {target_path}')
    except Exception as e:
        print(f'FAIL: File {s3_key} download fail : {e}')
    finally:
        return s3_key
