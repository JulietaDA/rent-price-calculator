import os
import sys
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from datetime import datetime

# Agrego el directorio principal al path para poder importar módulos de la carpeta tasks
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

# Importo las funciones desde la carpeta tasks/usd/
from tasks.usd.extract_usd import extract_data
from tasks.usd.transform_usd import transform_data
from tasks.usd.load_usd import load_data_to_redshift

# Para cada variable de BCRA, crear un DAG dinámico
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
                'casa': casa  
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