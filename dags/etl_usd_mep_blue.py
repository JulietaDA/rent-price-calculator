import os
import sys
import json
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from datetime import datetime

# Add the parent directory to the path
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

from tasks.usd.extract_usd import extract_data
from tasks.usd.transform_usd import transform_data
from tasks.usd.load_usd import load_data_to_redshift

def load_usd_variables_from_json(file_path):
    with open(file_path, 'r') as file:
        variables = json.load(file)
        # Maneja solo las variables relacionadas con "airflow_variables_usd"
        if "airflow_variables_usd" in variables:
            Variable.delete("airflow_variables_usd")  # Elimina solo la clave relevante
            Variable.set("airflow_variables_usd", json.dumps(variables["airflow_variables_usd"])) 

# Load environment variables and Airflow variables from the JSON file
load_usd_variables_from_json('./airflow_variables/airflow_variables_usd.json')

# Obtén las variables cargadas de Airflow
variables_usd = Variable.get("airflow_variables_usd", deserialize_json=True)


for var_name, var_config in variables_usd.items():
    casa = var_config['casa']
    schedule_interval = var_config['schedule']
    table_sufix = var_config['table_sufix']

    # Definir el DAG
    default_args = {
        'owner': 'julietada',
        'description': f'Pipeline para USD {var_name}, extraer y cargar datos en Redshift',
        'depends_on_past': False,
        'start_date': datetime(2023, 12, 31),
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 0, 
    }

    with DAG(f'usd_{var_name}', 
            default_args=default_args, 
            schedule_interval=schedule_interval,
            catchup=True,
            max_active_runs=1,) as dag:

        extract_task = PythonOperator(
            task_id=f'extract_data',
            python_callable=extract_data,
            op_kwargs={
                'casa': casa  # Pasar el idvariable desde la Variable de Airflow
            },
            provide_context=True,  # Ensure context is provided
        )

        transform_task = PythonOperator(
            task_id=f'transform_data',
            python_callable=transform_data,
            provide_context=True,  # You can also provide context here if needed
        )

        load_task = PythonOperator(
            task_id=f'load_data_to_redshift',
            python_callable=load_data_to_redshift,
            op_kwargs={
                'table_sufix': table_sufix   # Pasar el nombre de la tabla
            },            
            provide_context=True,  
        )

        extract_task >> transform_task >> load_task  # Set task dependencies

        # Registra el DAG dinámicamente
        globals()[f'dag_{var_name}'] = dag        