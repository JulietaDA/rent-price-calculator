import pandas as pd
from airflow.models import Variable

def transform(ti, **context):
    table_icl, table_usd_oficial, table_ipc, table_usd_blue, table_usd_mep = ti.xcom_pull(task_ids='extract')

    # Obtener la variable de pago inicial desde Airflow
    pago_inicial = float(Variable.get("pago_inicial"))

    # Definir un diccionario para los DataFrames y sus sufijos
    dataframes = {
        'df_icl': table_icl,
        'df_oficial': table_usd_oficial,
        'df_ipc': table_ipc,
        'df_blue': table_usd_blue,
        'df_mep': table_usd_mep
    }

    # Concatenar DataFrames y agregar sufijos
    concatenated_df = pd.concat(
        [df.add_suffix(f'_{name.split("_")[-1]}') for name, df in dataframes.items()],
        axis=1
    )
    print(f"Head: {concatenated_df.head()}")

    # Defino la variable de pago inicial
    concatenated_df["pago_inicial"] = pago_inicial

    concatenated_df["partition_date"] = context['ds']
    
    # Calcular el ajuste de renta
    concatenated_df["renta_ajustada"] = round(concatenated_df['pago_inicial'] * (1 + concatenated_df['dif_porcentual_icl']/100))
    # Calculo el valor para los diferentes dolares
    concatenated_df["renta_usd_oficial"] = round(concatenated_df['pago_inicial'] / concatenated_df['valor_actual_oficial'])
    concatenated_df["renta_usd_blue"] = round(concatenated_df['pago_inicial'] / concatenated_df['valor_actual_blue'])
    concatenated_df["renta_usd_mep"] = round(concatenated_df['pago_inicial'] / concatenated_df['valor_actual_mep'])    

    concatenated_df =  concatenated_df[['pago_inicial', 'renta_ajustada', 'dif_porcentual_icl', 'valor_acumulado_ipc',
                                        'valor_actual_oficial', 'dif_porcentual_oficial',
                                        'valor_actual_blue', 'dif_porcentual_blue',
                                        'valor_actual_mep', 'dif_porcentual_mep', 
                                        'renta_usd_oficial', 'renta_usd_blue', 'renta_usd_mep',
                                        'partition_date']]
    concatenated_df = concatenated_df.rename(columns={
                                            'pago_inicial': 'alquiler_inicial',
                                            'renta_ajustada': 'alquiler_hoy',
                                            'dif_porcentual_icl': 'porc_icl',
                                            'valor_acumulado_ipc': 'ipc_acum',
                                            'valor_actual_oficial': 'usd_ofic',
                                            'dif_porcentual_oficial': 'porc_ofic',
                                            'valor_actual_blue': 'usd_blue',
                                            'dif_porcentual_blue': 'porc_blue',
                                            'valor_actual_mep': 'usd_mep',
                                            'dif_porcentual_mep': 'porc_mep',
                                            'renta_usd_oficial': 'alquiler_usd_ofic',
                                            'renta_usd_blue': 'alquiler_usd_blue',
                                            'renta_usd_mep': 'alquiler_usd_mep'
    })
    print(f"Columnas: {concatenated_df.columns}")
    print(f"Head: {concatenated_df.head()}")    
    print(f"Shape: {concatenated_df.shape}")

    return concatenated_df