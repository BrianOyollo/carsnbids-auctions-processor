import boto3, botocore
import io,os,json, csv
import pandas as pd
import numpy as np
from dotenv import load_dotenv
import psycopg2
from sqlalchemy import create_engine
from airflow.providers.postgres.hooks.postgres import PostgresHook
import psycopg2
from psycopg2 import sql


script_dir = os.path.dirname(os.path.abspath(__file__))
env_file_path = os.path.expanduser("~/airflow/.env")
load_dotenv(dotenv_path=env_file_path)

# AWS
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID12')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY12')
RAW_AUCTIONS_BUCKET = os.getenv('RAW_AUCTIONS_BUCKET')

# DB
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_NAME = os.getenv('DB_NAME')


def enforce_column_types(df):
    # df['auction_date'] = pd.to_datetime(df['auction_date'], errors='coerce')
    numeric_cols = ['mileage', 'bid_count', 'highest_bid_value', 'manufacture_year', 'max_bid', 'min_bid']
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    df['reserve_met'] = df['reserve_met'].astype(bool)
    return df

def load_to_s3(s3_client, bucket, df)->list:
    """
    Uploads a cleaned DataFrame to an S3 bucket in NDJSON format, grouped by auction date.

    This function groups the input DataFrame by the `auction_date` column (date-only),
    and for each group:
      - Writes the group as NDJSON to memory.
      - Checks if a corresponding object (file) already exists in the S3 bucket.
        - If it exists: reads existing data, appends the new group data, and writes the merged data back to S3.
        - If it does not exist: uploads the new group data directly.
    
    Parameters:
    -----------
    s3_client : boto3.client
        A Boto3 S3 client instance.
    
    bucket : str
        The name of the target S3 bucket.

    df : pandas.DataFrame
        The cleaned DataFrame containing an 'auction_date' column in datetime format.
        The function creates a temporary column 'auction_saving_date' (date-only) to group the data.
    
    Returns:
        - a list of uploaded objects keys
    """
    uploaded_objects = []
    def check_object_exists(bucket,object_key):
        try:
            response = s3_client.head_object(Bucket=bucket, Key=object_key)
            return True
        except botocore.exceptions.ClientError as e:
            return False
        

    # group the df by auction_saving_date
    df['auction_saving_date'] = df['auction_date'].dt.date
    for auction_day, group in df.groupby('auction_saving_date'):
        # create object key
        group_object_key = f'{auction_day}.json'

        ndjson_str = group.to_json(orient='records', lines=True)
        new_data = [json.loads(line) for line in ndjson_str.splitlines()]


        # check if key exists in bucket
        if check_object_exists(bucket, group_object_key):
            response = s3_client.get_object(Bucket=bucket, Key=group_object_key)
            existing_data = [json.loads(line) for line in response['Body'].read().decode('utf-8').splitlines()]

            # combine existing and new auction data
            combined_data = existing_data + new_data

            # create df
            # sort data by auction_date in desc order
            # drop duplicates based on auction_id
            df = pd.DataFrame(combined_data)
            df = enforce_column_types(df) 
            df = df.drop(columns=['auction_saving_date']).sort_values('auction_date', ascending=False).reset_index(drop=True)
            df = df.drop_duplicates('auction_id', keep='first')


            # upload updated data back to s3
            ndjson_str = "\n".join(json.dumps(record) for record in df.to_dict(orient='records'))
            s3_client.put_object(Bucket=bucket, Key=group_object_key, Body=ndjson_str.encode('utf-8'), ContentType='application/json')
            uploaded_objects.append(group_object_key)

        else:
            ndjson_str = "\n".join(json.dumps(record) for record in new_data)
            s3_client.put_object(Bucket=bucket, Key=group_object_key, Body=ndjson_str, ContentType='application/json')
            uploaded_objects.append(group_object_key)

    return uploaded_objects

def psycopg_connection(user:str,password:str,host:str,port:int,db_name:str):
    connection = psycopg2.connect(
        dbname=db_name,
        user=user,
        password=password,
        host=host,
        port=port
        
    )
    cursor = connection.cursor()
    return connection, cursor

def close_psycopg_connection(conn, cursor):
    cursor.close()
    conn.close()

def load_to_postgres(df, conn, cursor):
        insert_columns = [
            "auction_date","auction_id","vin","seller_type","reserve_status","reserve_met","auction_status",
            "auction_title","auction_subtitle","make","model","exterior_color","interior_color",
            "body_style","mileage","engine","drivetrain","transmission","transmission_type", "gears",
            "title_status_cleaned","title_state","city","state","bid_count", "view_count", "watcher_count",
            "highest_bid_value","max_bid","min_bid","mean_bid","median_bid","bid_range","bids",
            "highlight_count","equipment_count","mod_count","flaw_count","service_count","included_items_count",
            "video_count","manufacture_year","location","auction_url","seller"  
        ]
        insert_df = df[insert_columns]
        
        insert_df = insert_df.replace({np.nan: None})
        insert_df = insert_df.sort_values('auction_date', ascending=False).reset_index(drop=True)
        insert_df = insert_df.drop_duplicates('auction_id', keep='first')

        data = list(insert_df.itertuples(index=False, name=None))
        query = sql.SQL("INSERT INTO {table} ({columns}) VALUES ({placeholders})").format(
            table = sql.Identifier('staging'),
            columns = sql.SQL(", ").join(map(sql.Identifier, insert_columns)),
            placeholders = sql.SQL(", ").join(sql.Placeholder()*len(insert_columns))
            
        )

        cursor.executemany(query, data)    
        conn.commit()
        return cursor.rowcount





def save_auctions_locally_by_date(df: pd.DataFrame, output_dir: str) -> list:
    """
    Saves auction data grouped by date to local files and returns the filenames.

    Parameters:
        df (pd.DataFrame): The auction data.
        output_dir (str): Directory where JSON files will be stored.

    Returns:
        list: List of saved filenames (e.g., ['2024-10-01.json', ...])
    """


    if not os.path.exists(output_dir):
        os.makedirs(output_dir)


    saved_files = []

    for auction_day, group in df.groupby('auction_date'):
        filename = f"{auction_day}.json"
        filepath = os.path.join(output_dir, filename)

        if os.path.exists(filepath):
            existing = pd.read_json(filepath, lines=True)
            combined = pd.concat([existing, group], ignore_index=True)
            combined.drop_duplicates(subset=['vin','auction_id'], inplace=True)
        else:
            combined = group

        combined.to_json(filepath, orient='records', lines=True)

        print(f"Saved {len(combined)} records to {filepath}")
        saved_files.append(filename)

    return saved_files


def load_to_local_csv(data:list, headers:list, filename:str, output_dir:str):
    """
    Saves data to a CSV file locally.

    Parameters:
        data (list): List of rows or dictionaries to write.
        headers (list): List of column headers.
        filename (str): file name for the csv file.
        output_dir (str): Directory where the CSV will be saved.
        
    Returns:
        str: Path to the saved CSV file.
    """
    # Ensure output directory exists
    os.makedirs(output_dir, exist_ok=True)

    # Generate filename with timestamp
    file_path = os.path.join(output_dir, filename)

    # Write to CSV
    with open(file_path, mode="w", newline="", encoding="utf-8") as outcsv:
        writer = csv.writer(outcsv)
        writer.writerow(headers)
        for row in data:
            writer.writerow(row)

    return file_path
