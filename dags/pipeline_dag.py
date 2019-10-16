import os
import logging
import requests
from datetime import datetime, timedelta
from copy import deepcopy
import json

from airflow import DAG
from airflow import models
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from api_utils.apis import IEXAPI
from api_utils.request_executors import IEXRequestExecutor

TICKERS = ['BTCUSDT', 'ETHUSDT', 'LTCUSDT']
YESTERDAY = datetime.combine(
    datetime.today() - timedelta(1),
    datetime.min.time()
)
MAPPED_DIRECTORY_PATH = '/home/airflow/gcs/'
OUTPUT_CSV_PATH = 'data/current_equity_price_{ticker}.csv'
PATH = os.path.join(MAPPED_DIRECTORY_PATH, OUTPUT_CSV_PATH)
AIRFLOW_BUCKET = models.Variable.get('AIRFLOW_BUCKET')
PROJECT = models.Variable.get('PROJECT_ID')
DATASET = models.Variable.get('DATASET')

DEFAULT_ARGS = {
    'start_date': YESTERDAY,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=5),
    'project_id': PROJECT,
}

def log_pipeline_initialization(**_context):
    logging.info('Initializing pipeline')

def write_current_price_file(**context):
    path = context['params']['path']
    tickers = context['params']['tickers']
    request_writer = IEXRequestExecutor(path, tickers)
    request_writer()

dag = DAG('airflow-pipeline',
    default_args=DEFAULT_ARGS,
    schedule_interval=timedelta(seconds=30),
    catchup=False
)

with dag:
    initialization_message_task = PythonOperator(
        task_id='initialization_message',
        python_callable=log_pipeline_initialization,
        provide_context=True,
        dag=dag,
    )
    create_price_file_task = PythonOperator(
        task_id='create_pricing_file',
        params={
            'path': PATH,
            'tickers': TICKERS,
        },
        python_callable=write_current_price_file,
        provide_context=True,
        dag=dag,
    )
    gcs_to_bq_task_list = [
        GoogleCloudStorageToBigQueryOperator(
            task_id=f'gcs_to_big_query_{ticker}',
            bucket=AIRFLOW_BUCKET,
            source_objects=[OUTPUT_CSV_PATH.format(ticker=ticker), ],
            destination_project_dataset_table=f'{PROJECT}:{DATASET}.price_data_{ticker}',
            source_format='CSV',
            write_disposition='WRITE_APPEND',
            create_disposition='CREATE_IF_NEEDED',
            schema_fields=[
                {'name': 'latestPrice', 'type': 'FLOAT', 'mode': 'NULLABLE'},
                {'name': 'latestUpdate', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
                {'name': 'symbol', 'type': 'STRING', 'mode': 'NULLABLE'},
            ],
            dag=dag,
        ) for ticker in TICKERS
    ]
    initialization_message_task >> create_price_file_task >> gcs_to_bq_task_list
