from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import logging


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

def check_minio_connection():
    try:
        hook = S3Hook('minio_conn')
        buckets = hook.get_bucket_list()
        logging.info(f"Connected to MinIO! Found {len(buckets)} buckets:")
        for bucket in buckets:
            logging.info(f"  - {bucket['Name']}")
        return True
    except Exception as e:
        logging.error(f"Failed to connect to MinIO: {e}")
        raise

def list_files_in_bucket(bucket_name, **context):
    hook = S3Hook('minio_conn')
    
    logging.info(f"Listing files in bucket: {bucket_name}")
    
    try:
        # Получить список объектов
        objects = hook.list_keys(bucket_name=bucket_name)
        
        if objects:
            logging.info(f"Found {len(objects)} files:")
            for obj in objects:
                logging.info(f"  - {obj}")
        else:
            logging.info(f"No files found in bucket '{bucket_name}'")
        
        # Сохранить результат в XCom
        context['ti'].xcom_push(key='file_list', value=objects)
        return objects
        
    except Exception as e:
        logging.error(f"Error listing bucket {bucket_name}: {e}")
        raise

def count_files_in_bucket(bucket_name, **context):

    hook = S3Hook('minio_conn')
    
    try:
        objects = hook.list_keys(bucket_name=bucket_name)
        count = len(objects) if objects else 0
        logging.info(f"Bucket '{bucket_name}' contains {count} files")
        return count
    except Exception as e:
        logging.error(f"Error counting files in {bucket_name}: {e}")
        raise

with DAG(
    'minio_check_dag',
    default_args=default_args,
    schedule=None,  
    catchup=False,
    tags=['minio', 'check']
) as dag:
    
    check_connection = PythonOperator(
        task_id='check_minio_connection',
        python_callable=check_minio_connection,
    )
    
    list_prod_files = PythonOperator(
        task_id='list_prod_files',
        python_callable=list_files_in_bucket,
        op_kwargs={'bucket_name': 'prod'},
    )
    
    count_tickers_files = PythonOperator(
        task_id='count_tickers_files',
        python_callable=count_files_in_bucket,
        op_kwargs={'bucket_name': 'tickers'},
    )
    
    check_connection >> [list_prod_files, count_tickers_files]