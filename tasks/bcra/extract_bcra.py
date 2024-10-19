import requests
import pandas as pd
from airflow.macros import ds_add

def extract_data(idvariable, **context):
    # Calculate the dates
    date = context['ds']
    hasta = ds_add(date, -1)  
    desde = hasta  # Tienen la misma fecha, porque la idea es que corra para un dia especifico.

    print('DAG run’s logical date (partition_date): ', date)
    print('Date from the data: ', hasta)
    
    # Variable ID 
    print('idvariable: ', idvariable)

    # Construct the URL using the calculated dates
    BASE_URL = "https://api.bcra.gob.ar"
    url = f"{BASE_URL}/estadisticas/v2.0/datosvariable/{idvariable}/{desde}/{hasta}"

    print("URL: ", url)

    # Hago la request
    response = requests.get(url, verify=False)

    # Check if the request was successful
    if response.status_code == 200:
        print(f"Status code: {response.status_code}")
        data = response.json()  # Parse the JSON response
        df = pd.DataFrame(data['results'])
        print("df_size: ", df.size)
        return df
    else:
        # Raise an exception to fail the task if status code is not 200
        error_message = f"Failed to fetch data. Status code: {response.status_code}, Response: {response.text}"
        print(error_message)
        raise Exception(error_message)  # This will fail the DAG