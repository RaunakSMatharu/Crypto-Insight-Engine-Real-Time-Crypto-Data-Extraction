from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator,BigQueryCreateEmptyTableOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

import json
import requests
from datetime import datetime, timedelta
import pandas as pd


GCP_PROJECT='airflow-crypto'
GCS_BUCKET="crypto-exchange-pipeline-raunak"
GCS_RAW_DATA_PATH='raw_data/crypto_raw_data'
GCS_TRANSFORMWD_PATH='transformed_data/crypto_transformed_data'
BIG_QUERY_DATASET='crypto_db'
BIGQUERY_TABLE='tbl_crypto'
# BQ_SCHEMA=[ 
#     {'name': 'id', 'type': 'STRING', 'mode': 'NULLABLE'},
#     {'name': 'symbol', 'type': 'STRING', 'mode': 'NULLABLE'},
#     {'name': 'name', 'type': 'STRING', 'mode': 'NULLABLE'},
#     {'name': 'current_price', 'type': 'FLOAT', 'mode': 'NULLABLE'},
#     {'name': 'market_cap', 'type': 'INTEGER', 'mode': 'NULLABLE'},
#     {'name': 'total_volume', 'type': 'INTEGER', 'mode': 'NULLABLE'},
#     {'name': 'last_updated', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
#     {'name': 'timestamp', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'}
#     ]

BQ_SCHEMA = [
            {'name': 'id', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'symbol', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'name', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'current_price', 'type': 'FLOAT', 'mode': 'NULLABLE'},
            {'name': 'market_cap', 'type': 'FLOAT', 'mode': 'NULLABLE'},
            {'name': 'fully_diluted_valuation', 'type': 'FLOAT', 'mode': 'NULLABLE'},
            {'name': 'total_volume', 'type': 'FLOAT', 'mode': 'NULLABLE'},
            {'name': 'high_24h', 'type': 'FLOAT', 'mode': 'NULLABLE'},
            {'name': 'low_24h', 'type': 'FLOAT', 'mode': 'NULLABLE'},
            {'name': 'price_change_24h', 'type': 'FLOAT', 'mode': 'NULLABLE'},
            {'name': 'price_change_percentage_24h', 'type': 'FLOAT', 'mode': 'NULLABLE'},
            {'name': 'market_cap_change_24h', 'type': 'FLOAT', 'mode': 'NULLABLE'},
            {'name': 'market_cap_change_percentage_24h', 'type': 'FLOAT', 'mode': 'NULLABLE'},
            {'name': 'circulating_supply', 'type': 'FLOAT', 'mode': 'NULLABLE'},
            {'name': 'total_supply', 'type': 'FLOAT', 'mode': 'NULLABLE'},
            {'name': 'max_supply', 'type': 'FLOAT', 'mode': 'NULLABLE'},
            {'name': 'ath', 'type': 'FLOAT', 'mode': 'NULLABLE'},
            {'name': 'ath_change_percentage', 'type': 'FLOAT', 'mode': 'NULLABLE'},
            {'name': 'ath_date', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
            {'name': 'atl', 'type': 'FLOAT', 'mode': 'NULLABLE'},
            {'name': 'atl_change_percentage', 'type': 'FLOAT', 'mode': 'NULLABLE'},
            {'name': 'atl_date', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
            {'name': 'last_updated', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
            {'name': 'timestamp', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'}
        ]



default_args = {
    "owner": "raunak",
    "depends_on_past": False,
    "start_date": datetime(2024, 7, 13),
}

def _fetch_data_from_api():
    url="https://api.coingecko.com/api/v3/coins/markets"
    params={
        'vs_currency':'usd',
        'order':'market_cap_desc',
        'per_page':10,
        'page':10,
        'sparkline':False
    }
    response=requests.get(url,params=params)
    data=response.json()
    with open("crypto_data.json",'w') as f:
        json.dump(data,f)




dag = DAG(
    dag_id="crypto-etl-dag",
    default_args=default_args,
    description="ETL Process for crypto Data",
    schedule_interval=timedelta(minutes=10),
    start_date=datetime(2024,7,13),
    catchup=False,
)

fetch_data_operator= PythonOperator(
    task_id='fetch_data_from_api',
    python_callable=_fetch_data_from_api,
    dag=dag,
)

def _transform_data():
    with open("crypto_data.json",'r') as f:
        data=json.load(f)
        
    transformed_data = []
    for item in data:
        transformed_data.append({
            'id': item['id'],
            'symbol': item['symbol'],
            'name': item['name'],
            'current_price': round(float(item['current_price']), 2) if item['current_price'] is not None else None,
            'market_cap': round(float(item['market_cap']), 2) if item['market_cap'] is not None else None,
            'fully_diluted_valuation': round(float(item['fully_diluted_valuation']), 2) if item['fully_diluted_valuation'] is not None else None,
            'total_volume': round(float(item['total_volume']), 2) if item['total_volume'] is not None else None,
            'high_24h': round(float(item['high_24h']), 2) if item['high_24h'] is not None else None,
            'low_24h': round(float(item['low_24h']), 2) if item['low_24h'] is not None else None,
            'price_change_24h': round(float(item['price_change_24h']), 2) if item['price_change_24h'] is not None else None,
            'price_change_percentage_24h': round(float(item['price_change_percentage_24h']), 2) if item['price_change_percentage_24h'] is not None else None,
            'market_cap_change_24h': round(float(item['market_cap_change_24h']), 2) if item['market_cap_change_24h'] is not None else None,
            'market_cap_change_percentage_24h': round(float(item['market_cap_change_percentage_24h']), 2) if item['market_cap_change_percentage_24h'] is not None else None,
            'circulating_supply': round(float(item['circulating_supply']), 2) if item['circulating_supply'] is not None else None,
            'total_supply': round(float(item['total_supply']), 2) if item['total_supply'] is not None else None,
            'max_supply': round(float(item['max_supply']), 2) if item['max_supply'] is not None else None,
            'ath': round(float(item['ath']), 2) if item['ath'] is not None else None,
            'ath_change_percentage': round(float(item['ath_change_percentage']), 2) if item['ath_change_percentage'] is not None else None,
            'ath_date': item['ath_date'],
            'atl': round(float(item['atl']), 2) if item['atl'] is not None else None,
            'atl_change_percentage': round(float(item['atl_change_percentage']), 2) if item['atl_change_percentage'] is not None else None,
            'atl_date': item['atl_date'],
            'last_updated': item['last_updated'],
            'timestamp': datetime.utcnow().isoformat()
})
        # transformed_data.append({
        #     'id': item['id'],
        #     'symbol': item['symbol'],
        #     'name': item['name'],
        #     'current_price':float(item['current_price']),
        #     'market_cap':item['market_cap'],
        #     'total_volume': item['total_volume'],
        #     'last_updated': item['last_updated'],
        #     'timestamp': datetime.utcnow().isoformat()
        # })




        



        

    df=pd.DataFrame(transformed_data)

    df.to_csv("transformaed_data.csv",index=False)


create_bucket_task=GCSCreateBucketOperator(
    task_id='create_bucket',
    bucket_name=GCS_BUCKET,
    storage_class='STANDARD',
    location='US',
    gcp_conn_id='google_cloud_default',
    dag=dag,
)

upload_raw_data_to_gcs=LocalFilesystemToGCSOperator(
    task_id='upload_raw_data_to_gcs',
    src='crypto_data.json',
    dst=GCS_RAW_DATA_PATH+"_{{ts_nodash}}.json",
    bucket=GCS_BUCKET,
    gcp_conn_id='google_cloud_default',
    dag=dag,
)

transformed_data_task=PythonOperator(
    task_id='transformed_data_task',
    python_callable=_transform_data,
    dag=dag,
)

upload_transform_data_to_gcs=LocalFilesystemToGCSOperator(
    task_id='upload_transformed_data_to_gcs',
    src='transformaed_data.csv',
    dst=GCS_TRANSFORMWD_PATH+"_{{ts_nodash}}.csv",
    bucket=GCS_BUCKET,
    gcp_conn_id='google_cloud_default',
    dag=dag,
)

#create Bigquerydataset
create_bigquery_dataset_task=BigQueryCreateEmptyDatasetOperator(
    task_id='create_bigquery_dataset',
    dataset_id=BIG_QUERY_DATASET,
    gcp_conn_id='google_cloud_default',
    dag=dag,
)


#create BIGQUERY TABLE
create_bigquery_table_task =BigQueryCreateEmptyTableOperator(
    task_id='create_bigquery_table',
    dataset_id=BIG_QUERY_DATASET,
    table_id=BIGQUERY_TABLE,
    schema_fields=BQ_SCHEMA,
    gcp_conn_id='google_cloud_default',
    dag=dag,
)


# #load Data to BigQuery
# load_to_bigquery=GCSToBigQueryOperator(
#     task_id='load_to_bigquery',
#     bucket=GCS_BUCKET,
#     source_objects=[GCS_TRANSFORMWD_PATH+'_{{ts_nodash}}.csv'],
#     destination_project_dataset_table=f'{GCP_PROJECT}:{BIG_QUERY_DATASET}.{BIGQUERY_TABLE}',
#     source_format='csv',
#     write_disposition='WRITE_APPEND',
#     skip_leading_rows=1,
#     gcp_conn_id='google_cloud_default',
#     dag=dag,
# )

load_to_bigquery = GCSToBigQueryOperator(
    task_id='load_to_bigquery',
    bucket=GCS_BUCKET,
    source_objects=[GCS_TRANSFORMWD_PATH+'_{{ts_nodash}}.csv'],
    destination_project_dataset_table=f'{GCP_PROJECT}:{BIG_QUERY_DATASET}.{BIGQUERY_TABLE}',
    source_format='CSV',
    write_disposition='WRITE_APPEND',
    skip_leading_rows=1,
    gcp_conn_id='google_cloud_default',
    dag=dag,
)



fetch_data_operator>>create_bucket_task>>upload_raw_data_to_gcs

create_bucket_task>>transformed_data_task>>upload_transform_data_to_gcs>>create_bigquery_dataset_task>>create_bigquery_table_task
create_bigquery_table_task>>load_to_bigquery