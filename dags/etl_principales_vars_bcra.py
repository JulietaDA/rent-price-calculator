import os
import sys
import json
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from datetime import datetime

# Agrego el directorio principal al path para poder importar módulos de la carpeta tasks
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

# Importo las funciones de extract_bcra, transform_bcra y load_bcra (ETL) desde la carpeta tasks/bcra/
from tasks.bcra.extract_bcra import extract_data
from tasks.bcra.transform_bcra import transform_data
from tasks.bcra.load_bcra import load_data_to_redshift

# Función para cargar variables relacionadas con BCRA desde un archivo JSON a las variables de Airflow
def load_bcra_variables_from_json(file_path):
    with open(file_path, 'r') as file:
        variables = json.load(file)
        if "airflow_variables_bcra" in variables:
            Variable.delete("airflow_variables_bcra")  # Elimina la variable existente en Airflow si existe
            Variable.set("airflow_variables_bcra", json.dumps(variables["airflow_variables_bcra"])) # Configura la nueva variable

# Cargar variables de entorno desde el archivo JSON
load_bcra_variables_from_json('./airflow_variables/airflow_variables_bcra.json')

# Para cada variable de BCRA, crear un DAG dinámico
variables_bcra = Variable.get("airflow_variables_bcra", deserialize_json=True)

for var_name, var_config in variables_bcra.items():
    idvariable = var_config['idvariable']
    schedule_interval = var_config['schedule']
    table_sufix = var_config['table_sufix']

    # Definir el DAG
    default_args = {
        'owner': 'julietada',
        'description': f'Pipeline para {var_name}, extraer y cargar datos en Redshift',
        'depends_on_past': False,
        'start_date': datetime(2023, 12, 31),
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 0,
    }

    with DAG(f'principalesVars_{var_name}', 
            default_args=default_args, 
            schedule_interval=schedule_interval,
            catchup=True,
            max_active_runs=1,) as dag:

        extract_task = PythonOperator(
            task_id=f'extract_data',
            python_callable=extract_data,
            op_kwargs={
                'idvariable': idvariable 
            },
            provide_context=True,  
        )

        transform_task = PythonOperator(
            task_id=f'transform_data',
            python_callable=transform_data,
            provide_context=True,  
        )

        load_task = PythonOperator(
            task_id=f'load_data_to_redshift',
            python_callable=load_data_to_redshift,
            op_kwargs={
                'table_sufix': table_sufix  
            },            
            provide_context=True,  
        )
        # Defino el orden de las tareas en el DAG
        extract_task >> transform_task >> load_task  

        # Registrar el DAG dinámicamente en el entorno de ejecución de Airflow
        globals()[f'dag_{var_name}'] = dag        