import pandas as pd

# Incorporar una llamada a GET api.bcra.gob.ar/estadisticas/v2.0/principalesvariables para obtener cdSerie y descripcion
# En la tabla final deberiamos encontrar la descripcion --> Crear una base de datos SQL (por ejemplo, PostgreSQL), a la cual se le puede cargar una tabla manualmente, y utilizarla en el script.(Se puede colocar la creación en el docker-compose o dejando instrucciones de cómo crearla).
# Esto termina generando un esquema estrella tmb


def transform_data(ti, **context):
    df = ti.xcom_pull(task_ids='extract_data')
    if df is None:  # Check if extraction was successful
        raise ValueError("No data extracted. Aborting transformation.")

    df.rename(columns={'fecha':'partition_date_usd'}, inplace=True)

    # Get the partition date from the DAG's execution context
    execution_date = context['ds']
    print('partition_date: ', context['ds'])
    # Assign the partition date from the context
    df['partition_date'] = execution_date

    print("df_size: ", df.size)

    df = df[["casa", "compra", "venta", "partition_date_usd", "partition_date"]]

    return df