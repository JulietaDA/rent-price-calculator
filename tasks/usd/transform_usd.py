import pandas as pd
import os

def transform_data(ti, **context):
    # Recuperar el archivo Parquet transformado desde XCom
    parquet_file = ti.xcom_pull(task_ids='extract_data')
    if parquet_file is None:
        raise ValueError("No data extracted. Aborting transformation.")

    # Leer el archivo Parquet
    df = pd.read_parquet(parquet_file)


    df.rename(columns={'fecha':'partition_date_usd'}, inplace=True)

    # Get the partition date from the DAG's execution context
    execution_date = context['ds']
    print('partition_date: ', context['ds'])
    # Assign the partition date from the context
    df['partition_date'] = execution_date

    print("df_size: ", df.size)

    df = df[["casa", "compra", "venta", "partition_date_usd", "partition_date"]]

    # Definir el path base relativo al script actual
    base_dir = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
    # Construir la ruta donde se guardará el archivo parquet
    transformed_parquet_path = os.path.join(base_dir, 'data', f"transformed_{context['ds']}.parquet")    
    df.to_parquet(transformed_parquet_path, index=False)
    print(f"Transformed data saved to {transformed_parquet_path}")

    return transformed_parquet_path