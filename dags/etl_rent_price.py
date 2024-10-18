import os
import sys
import json
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from datetime import datetime

# Add the parent directory to the path
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

from tasks.rent.extract import extract
from tasks.rent.transform import transform
from tasks.rent.load import load

# Function to load variables from a JSON file
def load_variables_from_json(file_path):
    with open(file_path, 'r') as file:
        variables = json.load(file)
        for key, value in variables.items():
            Variable.set(key, value) 

# Load environment variables and Airflow variables from the JSON file
load_variables_from_json('./airflow_variables/airflow_variables_rent.json')

# Definir el DAG
default_args = {
    'owner': 'julietada',
    'description': f'Calculate the rent for the period using variables and previous DAG results',
    'depends_on_past': False,
    'start_date': datetime(2024, 2, 2),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0, 
}

with DAG(
        'calculate_rent_price',
        default_args=default_args,
        schedule_interval="0 9 5 */2 *",
        description='Calculate the rent for the period using variables and previous DAG results',
        catchup=True) as dag:

        extract_task = PythonOperator(
            task_id=f'extract',
            python_callable=extract,
            provide_context=True,  # Ensure context is provided
        )

        transform_task = PythonOperator(
            task_id=f'transform',
            python_callable=transform,
            provide_context=True,  # You can also provide context here if needed
        )

        load_task = PythonOperator(
            task_id=f'load',
            python_callable=load,          
            provide_context=True,  
        )

        extract_task >> transform_task >> load_task  # Set task dependencies
