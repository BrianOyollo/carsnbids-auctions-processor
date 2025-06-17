import pandas as pd
from dotenv import load_dotenv
import os
import boto3
import json

script_dir = os.path.dirname(os.path.abspath(__file__))
env_file_path = os.path.expanduser("~/airflow/.env")
load_dotenv(dotenv_path=env_file_path)


AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID12')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY12')
RAW_AUCTIONS_BUCKET = os.getenv('RAW_AUCTIONS_BUCKET')


def read_auction_file(object_key:str)->dict:
    """
    Reads and parses a JSON auction file from an S3 bucket.

    Connects to AWS S3 using credentials defined in the environment,
    retrieves the object specified by `object_key` from the RAW_AUCTIONS_BUCKET,
    and returns the content as a Python dictionary.

    Args:
        object_key (str): The key (path/filename) of the S3 object to read.

    Returns:
        dict: Parsed JSON content of the S3 object.
    """

    s3 = boto3.client(
        's3',
        aws_access_key_id = AWS_ACCESS_KEY_ID,
        aws_secret_access_key = AWS_SECRET_ACCESS_KEY
    )
    response = s3.get_object(Bucket=RAW_AUCTIONS_BUCKET, Key=object_key)
    content = response['Body'].read().decode('utf-8')
    data = json.loads(content)
    return data
