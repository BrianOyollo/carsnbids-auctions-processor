
from airflow.sdk import dag, task, task_group
import os, pendulum, sys
from datetime import datetime, timedelta
from pathlib import Path
from dotenv import load_dotenv
from datetime import datetime, timedelta
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.operators.empty import EmptyOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import boto3
import pandas as pd


from etl_scripts import extract as extract_module
from etl_scripts import transform as transform_module
from etl_scripts import load as load_module
from scraper import setup
from scraper import scrape_auction as scraper
from scraper import save_auctions


script_dir = os.path.dirname(os.path.abspath(__file__))
env_file_path = os.path.expanduser("~/airflow/.env")
load_dotenv(dotenv_path=env_file_path)

raw_auctions_bucket = os.getenv('RAW_AUCTIONS_BUCKET')
processed_auctions_bucket = os.getenv('PROCESSED_AUCTIONS_BUCKET')
rescraped_auctions_bucket = os.getenv('RESCRAPED_AUCTIONS_BUCKET')

DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_NAME = os.getenv('DB_NAME')

saving_date = datetime.now().date() - timedelta(days=1)
main_auction_file = f"auctions_{saving_date}.json"
rescraped_auction_obj_key = f"rescraped_{saving_date}.json"


default_args = {
    'owner': 'brian',
    'depends_on_past': False,
    'email': ['brian@example.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=10),
    'execution_timeout': timedelta(hours=2),
    'start_date': datetime(2024, 5, 1),
}

@dag(
    schedule='* * * * *', # update this
    catchup=False,
    tags=["cars&bids"],
    default_args = default_args
)
def carsnbids_dag():

    sense_auction_file_task = S3KeySensor(
        task_id="auction_file_sensor",
        aws_conn_id = 'aws_default',
        bucket_name = raw_auctions_bucket,
        bucket_key = main_auction_file,
        poke_interval = 60*3,
        timeout = 60*60*4,
    )
    @task_group(group_id = "transform_rescrape")
    def transform_rescrape_group(
        s3_client,
        main_object_key:str, 
        rescrape_object_key:str,
        raw_auctions_bucket:str=None,
        processed_auctions_bucket:str=None,
        rescraped_auctions_bucket:str=None
        ):
        @task(task_id="transform_task")
        def transform(s3_client, main_object_key:str, raw_auctions_bucket:str=None, processed_auctions_bucket:str=None):
            processed_auction_keys = []
            rescrape_urls = []

            if not main_object_key:
                return {"processed_auction_keys": processed_auction_keys, "rescrape_urls": rescrape_urls}

            # read file
            auction_data = transform_module.load_json_from_s3(s3_client, raw_auctions_bucket, main_object_key)

            # convert to a list of dicts
            transformed_data = transform_module.convert_to_list_dicts(auction_data)

            # create df
            df = transform_module.create_auction_df(transformed_data)

            # filter out bad urls
            filtered_df, rescrape_urls = transform_module.get_and_remove_invalid_auctions(df)
            if filtered_df.empty:
                return {"processed_auction_keys": processed_auction_keys, "rescrape_urls": rescrape_urls}

            # clean and transform filtered_df
            cleaned_df = transform_module.clean_and_transform(filtered_df)

            # upload transformed auctions to processed auctions bucket
            processed_auction_keys = load_module.load_to_s3(s3_client, processed_auctions_bucket, cleaned_df)
            
            return {"processed_auction_keys": processed_auction_keys, "rescrape_urls": rescrape_urls}
    
        @task(task_id="rescrape_task")
        def rescrape_auction_urls(s3_client, rescraped_auctions_bucket:str, rescrape_object_key:str, transform_results:dict):
            rescrape_urls = transform_results.get('rescrape_urls')
            if not rescrape_urls:
                return None
            
            driver = setup.driver_setup()
            rescrapred_auction_data = []
            
            for url in rescrape_urls:
                auction_data = scraper.scrape_auction_data(driver, url)
                rescrapred_auction_data.append(auction_data)

            uploaded_object_key = save_auctions.save_auction_data_to_s3(
                s3_client, rescraped_auctions_bucket, rescrapred_auction_data, rescrape_object_key
            )
            setup.driver_teardown(driver)

            return uploaded_object_key
        
        main_tranform_task = transform(
            s3_client=s3_client,
            main_object_key=main_object_key,
            raw_auctions_bucket=raw_auctions_bucket,
            processed_auctions_bucket=processed_auctions_bucket
        )
        rescrape_task = rescrape_auction_urls(
            s3_client=s3_client,
            rescraped_auctions_bucket=rescraped_auctions_bucket,
            rescrape_object_key=rescrape_object_key,
            transform_results=main_tranform_task
        )
        transform_rescraped_task = transform(
            s3_client=s3_client,
            main_object_key=rescrape_task,
            raw_auctions_bucket=rescraped_auctions_bucket,
            processed_auctions_bucket=processed_auctions_bucket
        )

        return {
            "main_transform": main_tranform_task,
            "rescrape_transform": transform_rescraped_task
        }

    @task(task_id="merge_processed_files_keys")
    def merge_processed_files_s3_keys(transform_group_results: dict):
        all_keys = []

        main_transform_obj_keys = transform_group_results['main_transform'].get('processed_auction_keys', [])
        rescrape_transform_obj_keys = transform_group_results['rescrape_transform'].get('processed_auction_keys', [])

        all_keys.extend(main_transform_obj_keys)
        all_keys.extend(rescrape_transform_obj_keys)

        return all_keys


    @task_group(group_id="load_group")
    def load_group(s3_client, object_keys:list):
        if not object_keys:
            return None

         # empty staging table before loading new data
        empty_staging = SQLExecuteQueryOperator(
            task_id="empty_staging_table",
            conn_id="postgres_default_local",
            sql="sql/empty_staging.sql",
        )

        @task(task_id="load_to_postgres_staging")
        def load_to_postgres_staging(s3_client, object_keys:list)->int:

            hook = PostgresHook(postgres_conn_id="postgres_default_local")
            conn = hook.get_conn()
            cursor = conn.cursor()
        
            try:
                # read auction file(s) from s3
                auction_data = []
                for key in object_keys:
                    data = transform_module.load_json_from_s3(s3_client,processed_auctions_bucket,key=key,ndjson=True)
                    auction_data.extend(data)

                # create a df
                df = pd.DataFrame(auction_data)

                # load to staging table
                inserted_rows = load_module.load_to_postgres(df, conn, cursor)
                return inserted_rows
            
            except Exception as e:
                raise

            finally:
                cursor.close()
                conn.close()

        # load dim and fact tables from staging
        load_fact_and_dims = SQLExecuteQueryOperator(
            task_id="load_fact_and_dims",
            conn_id="postgres_default_local",
            sql="sql/load_tables.sql",
        )

        load_to_staging = load_to_postgres_staging(s3_client, object_keys)
        empty_staging >> load_to_staging >> load_fact_and_dims
        return load_to_staging
    


    # s3  & connections
    s3_client = boto3.client('s3')

    # main workflow
    transform_extract = transform_rescrape_group(
        s3_client=s3_client,
        main_object_key=main_auction_file,
        rescrape_object_key=rescraped_auction_obj_key,
        raw_auctions_bucket=raw_auctions_bucket,
        processed_auctions_bucket=processed_auctions_bucket,
        rescraped_auctions_bucket=rescraped_auctions_bucket
    )

    merged_keys_task = merge_processed_files_s3_keys(transform_extract)
    load_task = load_group(s3_client=s3_client, object_keys=merged_keys_task)

    sense_auction_file_task >> merged_keys_task >> load_task

carsnbids_dag()
