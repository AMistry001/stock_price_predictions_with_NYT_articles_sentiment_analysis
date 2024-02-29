import requests
import json
import time
from pymongo import MongoClient
from google.cloud import storage
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime


def get_data_to_gcs():
    base_url = "https://api.nytimes.com/svc/search/v2/articlesearch.json"
    api_key = "NYT API KEY" # update with your API key here

    meta_data = []
    for q in ['ABC', 'XYZ']: #update with list of stocks here
        for i in range(100):
            params = {
                'q': q,
                'begin_date':'20200101',
                'end_date':'20241231',
                'page':i,
                "api-key": api_key
            }
            response = requests.get(base_url, params=params)
            if response.status_code == 200:
                data = response.json()
                articles = data["response"]["docs"]
                for article in articles:
                    meta_data.append(article)
            else:
                time.sleep(12)


    project_id = "msds-697-spring-2024-412419"
    bucket_name = "msds697_group11"
    filename = 'nytimes.json'

    data = json.dumps(meta_data, indent=4)
    client = storage.Client(project=project_id)
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(filename)
    blob.upload_from_string(data)

def read_data_from_gcs_and_write_to_mongo():
    project_id = "msds-697-spring-2024-412419"
    bucket_name = "msds697_group11"
    filename = 'nytimes.json'

    # Download data from GCS
    client = storage.Client(project=project_id)
    bucket = client.get_bucket(bucket_name)
    blob = bucket.blob(filename)
    data = json.loads(blob.download_as_string())

    client = MongoClient("mongodb+srv://....") #update with your mongodb cluster name, password

    # Write data to MongoDB
    db = client["msds697_database_nytimes"]
    collection = db["msds697_collection_nytimes"]
    collection.insert_many(data)
    client.close()


with DAG(
    dag_id="msds697-group11",
    schedule='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False
) as dag:
    get_data_to_gcs_dag = PythonOperator(task_id = "get_data_to_gcs_dag",
                                                  python_callable = get_data_to_gcs,
                                                  dag=dag)

    read_data_from_gcs_and_write_to_mongo_dag = PythonOperator(task_id = "read_data_from_gcs_and_write_to_mongo_dag",
                                                    python_callable = read_data_from_gcs_and_write_to_mongo,
                                                    dag=dag)
    
    get_data_to_gcs_dag >> read_data_from_gcs_and_write_to_mongo_dag