from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from dag_tasks import *
import os

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': True,
    'email': [os.getenv('ALERTS_EMAIL', 'ahgarawanii@gmail.com')],
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

# DAG definition
with DAG(
    'orders_etl_dag',
    default_args=default_args,
    description='An end-to-end pipeline to extract, transform, load and visualize a dataset of video game sales',
    schedule_interval=timedelta(days=1),
    catchup=False,
) as dag:

    # Stage 1: Download the dataset and store it in local storage temporarily
    extract_dataset_task = PythonOperator(
        task_id='extract_dataset', 
        python_callable=extract_dataset
    )

    # Stage 2: Flatten the dataset and save it to local disk temporarily
    flatten_dataset_task = PythonOperator(
        task_id='flatten_dataset', 
        python_callable=flatten_dataset
    )

    # Stage 3: Transform the dataset and save the results as CSVs in local disk temporarily
    transform_dim_category_task = PythonOperator(
        task_id='transform_dim_category',
        python_callable=transform_dim_category
    )

    transform_dim_tag_task = PythonOperator(
        task_id='transform_dim_tag',
        python_callable=transform_dim_tag
    )
    
    transform_dim_product_review_task = PythonOperator(
        task_id='transform_dim_product_review',
        python_callable=transform_dim_product_review
    )

    transform_dim_product_task = PythonOperator(
        task_id='transform_dim_product',
        python_callable=transform_dim_product
    )

    transform_bridge_product_tag_task = PythonOperator(
        task_id='transform_bridge_product_tag',
        python_callable=transform_bridge_product_tag
    )

    transform_dim_customer_demo_task = PythonOperator(
        task_id='transform_dim_customer_demo',
        python_callable=transform_dim_customer_demo
    )

    transform_dim_address_task = PythonOperator(
        task_id='transform_dim_address',
        python_callable=transform_dim_address
    )

    transform_dim_customer_task = PythonOperator(
        task_id='transform_dim_customer',
        python_callable=transform_dim_customer
    )

    transform_fact_sales_task = PythonOperator(
        task_id='transform_fact_sales',
        python_callable=transform_fact_sales
    )

    # Stage 4: Load the transformed data into the data warehouse
    # For each table, create a load task that uses the CSV file whose name exactly
    # matches the table name.
    load_dim_category_task = PythonOperator(
        task_id='load_dim_category',
        python_callable=lambda **kw: load_csv_to_mysql('dim_category', **kw)
    )

    load_dim_tag_task = PythonOperator(
        task_id='load_dim_tag',
        python_callable=lambda **kw: load_csv_to_mysql('dim_tag', **kw)
    )

    load_dim_product_task = PythonOperator(
        task_id='load_dim_product',
        python_callable=lambda **kw: load_csv_to_mysql('dim_product', **kw)
    )

    load_bridge_product_tag_task = PythonOperator(
        task_id='load_bridge_product_tag',
        python_callable=lambda **kw: load_csv_to_mysql('bridge_product_tag', **kw)
    )

    load_dim_customer_demo_task = PythonOperator(
        task_id='load_dim_customer_demo',
        python_callable=lambda **kw: load_csv_to_mysql('dim_customer_demo', **kw)
    )

    load_dim_address_task = PythonOperator(
        task_id='load_dim_address',
        python_callable=lambda **kw: load_csv_to_mysql('dim_address', **kw)
    )

    load_dim_customer_task = PythonOperator(
        task_id='load_dim_customer',
        python_callable=lambda **kw: load_csv_to_mysql('dim_customer', **kw)
    )

    load_fact_sales_task = PythonOperator(
        task_id='load_fact_sales',
        python_callable=lambda **kw: load_csv_to_mysql('fact_sales', **kw)
    )

    load_dim_product_review_task = PythonOperator(
        task_id='load_dim_product_review',
        python_callable=lambda **kw: load_csv_to_mysql('dim_product_review', **kw)
    )

    # Task specifically designed to fail for testing email failure notifications
    def to_fail():
        raise Exception("This task is designed to fail for testing purposes.")

    failure_task = PythonOperator(
        task_id='failure_task',
        python_callable=to_fail
    )

    # Transformation tasks that can run in parallel
    transform_tasks = [
        transform_dim_category_task,
        transform_dim_tag_task,
        transform_dim_product_review_task,
        transform_dim_address_task,
        transform_dim_customer_demo_task,
        transform_dim_customer_task,
        transform_fact_sales_task
    ]

    # Define task dependencies
    extract_dataset_task >> flatten_dataset_task
    flatten_dataset_task >> transform_tasks
    transform_dim_category_task >> transform_dim_product_task
    [transform_dim_tag_task, transform_dim_product_task] >> transform_bridge_product_tag_task

    transform_dim_category_task >> load_dim_category_task
    transform_dim_tag_task >> load_dim_tag_task
    transform_dim_customer_demo_task >> load_dim_customer_demo_task
    transform_dim_address_task >> load_dim_address_task
    
    [load_dim_category_task, load_dim_tag_task, transform_dim_product_task] >> load_dim_product_task
    [transform_dim_product_review_task, load_dim_product_task] >> load_dim_product_review_task
    [transform_bridge_product_tag_task, load_dim_tag_task, load_dim_product_task] >> load_bridge_product_tag_task
    [transform_dim_customer_task, load_dim_customer_demo_task, load_dim_address_task] >> load_dim_customer_task
    [load_dim_customer_task, load_dim_product_task] >> load_fact_sales_task

    # Add failure_task to the DAG
    extract_dataset_task >> failure_task