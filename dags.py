import requests
import json
import time
from pymongo import MongoClient
from google.cloud import storage
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

def fetch_articles_and_upload_to_gcs(api_key, project_id, bucket_name, filename, keywords=['Election', 'Stock', 'Covid'], pages=50, start_date='20200101', end_date='20241231'):
    """
    Fetch articles from the New York Times API for given keywords and date range,
    then upload the fetched data to Google Cloud Storage.
    
    Parameters:
    - api_key: API key for the New York Times API.
    - project_id: GCP project ID.
    - bucket_name: GCS bucket name.
    - filename: The name of the file to create in GCS.
    - keywords: List of keywords to search for.
    - pages: Number of pages to fetch for each keyword.
    - start_date: Start date for the search query.
    - end_date: End date for the search query.
    """
    base_url = "https://api.nytimes.com/svc/search/v2/articlesearch.json"
    meta_data = []
    for q in keywords:
        for page in range(pages):
            params = {
                'q': q,
                'begin_date': start_date,
                'end_date': end_date,
                'page': page,
                'api-key': api_key
            }
            try:
                response = requests.get(base_url, params=params)
                response.raise_for_status()
                data = response.json()
                meta_data.extend(data["response"]["docs"])
            except requests.RequestException as e:
                time.sleep(10)
    
    # Upload the fetched data to Google Cloud Storage
    client = storage.Client(project=project_id)
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(filename)
    blob.upload_from_string(json.dumps(meta_data, indent=4))

def download_from_gcs_and_write_to_mongo(project_id, bucket_name, filename, uri, db_name, collection_name):
    """
    Download data from Google Cloud Storage and write it to MongoDB.
    
    Parameters:
    - project_id: GCP project ID.
    - bucket_name: GCS bucket name.
    - filename: The name of the file in GCS.
    - uri: MongoDB URI.
    - db_name: Name of the database.
    - collection_name: Name of the collection.
    """
    # Download data from GCS
    client = storage.Client(project=project_id)
    bucket = client.get_bucket(bucket_name)
    blob = bucket.blob(filename)
    data = json.loads(blob.download_as_string())

    # Write data to MongoDB
    mongo_client = MongoClient(uri)
    db = mongo_client[db_name]
    collection = db[collection_name]
    collection.insert_many(data)
    mongo_client.close()

# Airflow DAG setup
with DAG(dag_id="msds697-group11", schedule_interval='@daily', start_date=datetime(2024, 1, 1), catchup=False) as dag:
    fetch_and_upload_task = PythonOperator(
        task_id="fetch_articles_and_upload_to_gcs",
        python_callable=fetch_articles_and_upload_to_gcs,
        op_kwargs={'api_key': 'YOUR_API_KEY', 'project_id': 'YOUR_PROJECT_ID', 'bucket_name': 'YOUR_BUCKET_NAME', 'filename': 'nytimes.json'}
    )

    download_and_write_to_mongo_task = PythonOperator(
        task_id="download_from_gcs_and_write_to_mongo",
        python_callable=download_from_gcs_and_write_to_mongo,
        op_kwargs={'project_id': 'YOUR_PROJECT_ID', 'bucket_name': 'YOUR_BUCKET_NAME', 'filename': 'nytimes.json', 'uri': 'YOUR_MONGO_URI', 'db_name': 'YOUR_DB_NAME', 'collection_name': 'YOUR_COLLECTION_NAME'}
    )

    fetch_and_upload_task >> download_and_write_to_mongo_task
