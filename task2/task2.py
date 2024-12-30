import logging as log
import pandas as pd
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

# DAG Configuration
dag = DAG(
    "etl_postgres_to_s3_transform",
    schedule_interval='0 18 * * *',
    tags=['postgres', 's3', 'ETL'],
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
)


INTERMEDIATE_BUCKET = "intermediate"
TRANSFORMED_BUCKET = "target"
RAW_KEY = "raw_data.csv"
TRANSFORMED_KEY = "transformed_data.csv"


def extract_to_s3(**kwargs):
    postgres_hook = PostgresHook(postgres_conn_id="postgres_provider")
    sql_query = "SELECT * FROM source_table;"  # Replace with your table name
    connection = postgres_hook.get_conn()
    cursor = connection.cursor()
    cursor.execute(sql_query)
    data = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]
    cursor.close()
    connection.close()
    log.info(f"Extracted {len(data)} rows from Postgres")

    # Convert to DataFrame and save to S3
    df = pd.DataFrame(data, columns=columns)
    s3_hook = S3Hook(aws_conn_id="aws_default")
    csv_data = df.to_csv(index=False)
    s3_hook.load_string(csv_data, RAW_KEY, INTERMEDIATE_BUCKET, replace=True)
    log.info(f"Raw data written to S3: s3://{INTERMEDIATE_BUCKET}/{RAW_KEY}")

# Function: Transform data from S3 and save back to S3
def transform_data_s3(**kwargs):
    s3_hook = S3Hook(aws_conn_id="aws_default")
    # Load raw data from S3
    raw_data = s3_hook.read_key(key=RAW_KEY, bucket_name=INTERMEDIATE_BUCKET)
    df = pd.read_csv(pd.compat.StringIO(raw_data))

    # Perform transformation
    log.info("Starting data transformation")
    transformed_data = df[df['column_name'] != 'filter_value']  # Replace with real logic
    transformed_data['new_column'] = "transformed"
    log.info("Data transformed successfully")

    # Save transformed data back to S3
    csv_data = transformed_data.to_csv(index=False)
    s3_hook.load_string(csv_data, TRANSFORMED_KEY, TRANSFORMED_BUCKET, replace=True)
    log.info(f"Transformed data written to S3: s3://{TRANSFORMED_BUCKET}/{TRANSFORMED_KEY}")


extract_to_s3_task = PythonOperator(
    task_id='extract_to_s3',
    python_callable=extract_to_s3,
    provide_context=True,
    dag=dag,
)

transform_data_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data_s3,
    provide_context=True,
    dag=dag,
)


extract_to_s3_task >> transform_data_task