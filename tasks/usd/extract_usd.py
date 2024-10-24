import requests
import pandas as pd
from airflow.macros import ds_add, ds_format
import os

def extract_data(casa, **context):
    # Obtengo las fechas
    date = context['ds']
    fecha = ds_format(ds_add(date, -1), "%Y-%m-%d", "%Y/%m/%d")  # Tienen la misma fecha, porque la idea es que corra para un dia especifico

    print('DAG run’s logical date (partition_date): ', date)
    print('Date from the data: ', fecha)
    
    # Casa: Blue - MEP
    print('Casa: ', casa)

    # Construct the URL using the calculated dates
    BASE_URL = "https://api.argentinadatos.com"
    url = f"{BASE_URL}/v1/cotizaciones/dolares/{casa}/{fecha}"

    print("URL: ", url)

    # Make the request
    response = requests.get(url, verify=False)

    # Check if the request was successful
    if response.status_code == 200:
        print(f"Status code: {response.status_code}")
        data = response.json()  # Parse the JSON response
        df = pd.DataFrame([data])
        print("df_size: ", df.size)

        # Definir el path base relativo al script actual
        base_dir = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
        # Construir la ruta donde se guardará el archivo parquet
        parquet_path = os.path.join(base_dir, 'data', f"{casa}_data_{date}.parquet")
        df.to_parquet(parquet_path, index=False)
        print(f"Data saved to {parquet_path}")    
    
        return parquet_path
    else:
        # Raise an exception to fail the task if status code is not 200
        error_message = f"Failed to fetch data. Status code: {response.status_code}, Response: {response.text}"
        print(error_message)
        raise Exception(error_message)  # This will fail the DAG