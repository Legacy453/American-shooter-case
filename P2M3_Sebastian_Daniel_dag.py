import psycopg2
import re
import pandas as pd
import numpy as np
import datetime as dt
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from elasticsearch import Elasticsearch

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

dag = DAG(
    'm3_dag',
    default_args=default_args,
    description='DAG untuk milestone 3',
    schedule=None,  # Set your desired schedule interval
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=None,
    tags=['data-processing'],
)

def get_table(database, user, password, host, port, table_name):
    # Establish a connection to the PostgreSQL database
    conn = psycopg2.connect(
        database=database,
        user=user,
        password=password,
        host=host,
        port=port
    )

    try:
        # Create a cursor object using the cursor() method
        cur = conn.cursor()

        # Execute a PostgreSQL query
        cur.execute(f"SELECT * FROM table_m3")

        # Fetch all rows from the result set
        rows = cur.fetchall()

        df = pd.DataFrame(rows, columns=[desc[0] for desc in cur.description])

        return df

    except Exception as e:
        print(f"Error: {e}")

    finally:
        # Close communication with the PostgreSQL database
        if cur:
            cur.close()
        if conn:
            conn.close()

def clean_table(df):
    df.drop_duplicates(inplace= True)

    df.columns = df.columns.str.strip()

    df.rename(columns=lambda x: re.sub('([a-z])([A-Z])', r'\1_\2', x).lower(), inplace=True)

    df['location'] = df['location'].fillna("on title")

    df['incident_area'] = df['incident_area'].fillna("unknown")

    df['open_close_location'] = df['open_close_location'].fillna("close")

    df['target'] = df['target'].fillna("unknown")

    df['cause'] = df['cause'].fillna("unknown")

    df['policeman_killed'] = df['policeman_killed'].fillna("0")

    df['incident_area'] = df['incident_area'].fillna("unknown")

    #filling NA in "age" using random number
    start_range = 18
    end_range = 50

    mask = df['age'].isnull()
    num_nan = mask.sum()
    random_numbers = np.random.randint(start_range, end_range + 1, num_nan)
    df.loc[mask, 'age'] = random_numbers

    df['employeed'] = df['employeed'].fillna("false")

    df['employed_at'] = df['employed_at'].fillna("unknown")

    df['race'] = df['race'].fillna("unknown")

    df.drop(columns=['latitude'], inplace=True)

    df.drop(columns=['longitude'], inplace=True)

    return df

def migrate_to_elasticsearch():
    # Load cleaned data from CSV
    cleaned_data = pd.read_csv('P2M3_Sebastian_Daniel_data_clean.csv')

    # Establish connection to Elasticsearch
    es = Elasticsearch("http://localhost:9200")

    # Iterate through DataFrame rows and index data into Elasticsearch
    for i, r in cleaned_data.iterrows():
        doc = r.to_json()
        res = es.index(index="mc3_test_1", doc_type="_doc", body=doc)  # Index data in Elasticsearch
        print(res)


database = "Hacktiv8_P2"
user = "postgres"
password = "Glance@u1"
host = "localhost"
port = "5432"
table_name = "table_m3"

fetch_and_clean_data_task = PythonOperator(
    task_id='fetch_and_clean_data',
    python_callable=lambda: clean_table(get_table(database, user, password, host, port, table_name)),
    dag=dag
)

migrate_to_elasticsearch_task = PythonOperator(
    task_id='migrate_to_elasticsearch',
    python_callable=migrate_to_elasticsearch,
    dag=dag
)

# Set task dependencies
fetch_and_clean_data_task >> migrate_to_elasticsearch_task