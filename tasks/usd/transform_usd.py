import pandas as pd

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