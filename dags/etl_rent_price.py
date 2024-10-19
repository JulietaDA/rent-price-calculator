import os
import sys
import json
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from datetime import datetime

# Agrego el directorio principal al path para poder importar módulos de la carpeta tasks
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

# Importo las funciones desde la carpeta tasks/rent/
from tasks.rent.extract import extract
from tasks.rent.transform import transform
from tasks.rent.load import load

# Función para cargar variables desde un archivo JSON a las variables de Airflow
def load_variables_from_json(file_path):
    with open(file_path, 'r') as file:
        variables = json.load(file)
        for key, value in variables.items():
            Variable.set(key, value) 

# Cargar variables de entorno desde el archivo JSON
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
            provide_context=True, 
        )

        transform_task = PythonOperator(
            task_id=f'transform',
            python_callable=transform,
            provide_context=True, 
        )

        load_task = PythonOperator(
            task_id=f'load',
            python_callable=load,          
            provide_context=True,  
        )
        
        # Defino el orden de las tareas en el DAG
        extract_task >> transform_task >> load_task  
