import os
import boto3
from dotenv import load_dotenv

from .constants import UNZIPPED_FILES_FOLDER, DUMMY_FILES_SUB_KEY

def clean_s3_bucket(bucket_name, key):
    s3 = boto3.resource('s3')
    s3.Object(bucket_name, key).delete()
    print(f'{key} is deleted from bucket {bucket_name}')


def upload_to_s3( grouping_folder, file_name):
    load_dotenv()

    aws_access_key_id = os.getenv('ACCESS_KEY')
    aws_secret_access_key = os.getenv('SECRET_KEY')
    endpoint_url = os.getenv('ENDPOINT')
    cwd = os.getcwd()
    full_file_path = os.path.join(cwd, UNZIPPED_FILES_FOLDER, grouping_folder, file_name)

    s3_client = boto3.client(
            's3',
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            endpoint_url=endpoint_url
        )
    s3_resource = boto3.resource(
        's3',
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        endpoint_url=endpoint_url
    )

    key = f"{DUMMY_FILES_SUB_KEY}/{grouping_folder}/{file_name}"
        
    ## uncomment if you want to celan a bucket clean s3 bucket
    # for file in my_bucket.objects.all():
    #             s3_resource.Object(os.getenv('BUCKET_NAME'), str(file)).delete()
    #             print(file)

    s3_resource.Bucket(os.getenv('BUCKET_NAME')).put_object(
                Key=key,
                Body=open(full_file_path, 'rb')
            )
    return key