import pandas as pd
import os
import yaml
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from airflow.models import Variable
from datetime import datetime
from airflow.macros import ds_add

def extract(**context):
    # Cargar variables de entorno desde el archivo .env
    load_dotenv()

    # Cargar el archivo config.yml
    config_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../config.yml'))
    with open(config_path, 'r') as file:
        config = yaml.safe_load(file)

    # Acceder a los valores de configuración
    host = config['database']['host']
    port = config['database']['port']
    database = config['database']['dbname']
    db_user = config['database']['dbuser']
    db_password = os.getenv('DB_PASSWORD')  # Información sensible desde variables de entorno

    # Defino el esquema
    schema_name = f"{db_user}_schema"

    # Crear la URL de conexión
    database_url = f"postgresql+psycopg2://{db_user}:{db_password}@{host}:{port}/{database}"

    try:
        # Creo un motor de SQLAlchemy
        engine = create_engine(database_url)

        # Usar el contexto de conexión
        with engine.connect() as connection:
            # Obtener fechas desde las variables de Airflow
            fecha_inicio_str = Variable.get("fecha_inicio")
            fecha_inicio = datetime.strptime(fecha_inicio_str, '%Y-%m-%d').date()  # Convertir a objeto date
            fecha_actual = datetime.strptime(context['ds'], '%Y-%m-%d').date()
            print(f"Fecha inicio: {fecha_inicio}")
            print(f"Fecha actual: {fecha_actual}")

            # Consultas SQL con placeholders
            # Datos del ICL
            table_icl = pd.read_sql(text(f"""
                WITH valores AS (
                    SELECT 
                        valor AS valor,
                        'inicial' AS tipo
                    FROM 
                        "{schema_name}".principales_vars_icl icl
                    INNER JOIN (
                        SELECT MAX(partition_date) AS max_partition_date 
                        FROM "{schema_name}".principales_vars_icl
                        WHERE partition_date BETWEEN :fecha_inicio - INTERVAL '7 days' AND :fecha_inicio
                    ) part ON icl.partition_date = part.max_partition_date
                    UNION ALL
                    SELECT 
                        valor AS valor,
                        'posterior' AS tipo
                    FROM 
                        "{schema_name}".principales_vars_icl icl
                    INNER JOIN (
                        SELECT MAX(partition_date) AS max_partition_date 
                        FROM "{schema_name}".principales_vars_icl
                        WHERE partition_date BETWEEN :fecha_actual - INTERVAL '7 days' AND :fecha_actual
                    ) part ON icl.partition_date = part.max_partition_date
                )
                SELECT
                    (MAX(CASE WHEN tipo = 'posterior' THEN valor END) - 
                    MAX(CASE WHEN tipo = 'inicial' THEN valor END)) / 
                    MAX(CASE WHEN tipo = 'inicial' THEN valor END) * 100 AS dif_porcentual
                FROM
                    valores; 
            """), connection, params={"fecha_inicio": fecha_inicio, "fecha_actual": fecha_actual})

            # Datos del dolar oficial 
            table_usd_oficial = pd.read_sql(text(f"""
                WITH valores AS (
                    SELECT 
                        valor AS valor,
                        'inicial' AS tipo
                    FROM 
                        "{schema_name}".principales_vars_usd_oficial usd
                    INNER JOIN (
                        SELECT MAX(partition_date) AS max_partition_date 
                        FROM "{schema_name}".principales_vars_usd_oficial
                        WHERE partition_date BETWEEN :fecha_inicio - INTERVAL '7 days' AND :fecha_inicio
                    ) part ON usd.partition_date = part.max_partition_date
                    UNION ALL
                    SELECT 
                        valor AS valor,
                        'posterior' AS tipo
                    FROM 
                        "{schema_name}".principales_vars_usd_oficial usd
                    INNER JOIN (
                        SELECT MAX(partition_date) AS max_partition_date 
                        FROM "{schema_name}".principales_vars_usd_oficial
                        WHERE partition_date BETWEEN :fecha_actual - INTERVAL '7 days' AND :fecha_actual
                    ) part ON usd.partition_date = part.max_partition_date
                )
                SELECT
                    MAX(CASE WHEN tipo = 'posterior' THEN valor END) as valor_actual,
                    (MAX(CASE WHEN tipo = 'posterior' THEN valor END) - 
                    MAX(CASE WHEN tipo = 'inicial' THEN valor END)) / 
                    MAX(CASE WHEN tipo = 'inicial' THEN valor END) * 100 AS dif_porcentual
                FROM
                    valores; 
            """), connection, params={"fecha_inicio": fecha_inicio, "fecha_actual": fecha_actual})

            # Inflacion acumulada
            table_ipc = pd.read_sql(text(f"""
                WITH ipc_values AS (
                    SELECT 
                        partition_date, 
                        valor / 100 as ipc
                    FROM 
                        "{schema_name}".principales_vars_ipc
                    WHERE 
                        partition_date BETWEEN :fecha_inicio AND :fecha_actual
                )
                SELECT (EXP(SUM(LN(1 + ipc))) - 1) * 100 as valor_acumulado 
                FROM ipc_values
            """), connection, params={"fecha_inicio": fecha_inicio, "fecha_actual": fecha_actual})

            # Dolar blue
            table_usd_blue = pd.read_sql(text(f"""
                WITH valores AS (
                    SELECT 
                        venta AS valor,
                        'inicial' AS tipo
                    FROM 
                        "{schema_name}".usd_blue usd
                    INNER JOIN (
                        SELECT MAX(partition_date) AS max_partition_date 
                        FROM "{schema_name}".usd_blue
                        WHERE partition_date BETWEEN :fecha_inicio - INTERVAL '7 days' AND :fecha_inicio
                    ) part ON usd.partition_date = part.max_partition_date
                    UNION ALL
                    SELECT 
                        venta AS valor,
                        'posterior' AS tipo
                    FROM 
                        "{schema_name}".usd_blue usd
                    INNER JOIN (
                        SELECT MAX(partition_date) AS max_partition_date 
                        FROM "{schema_name}".usd_blue
                        WHERE partition_date BETWEEN :fecha_actual - INTERVAL '7 days' AND :fecha_actual
                    ) part ON usd.partition_date = part.max_partition_date
                )
                SELECT
                    MAX(CASE WHEN tipo = 'posterior' THEN valor END) as valor_actual,
                    (MAX(CASE WHEN tipo = 'posterior' THEN valor END) - 
                    MAX(CASE WHEN tipo = 'inicial' THEN valor END)) / 
                    MAX(CASE WHEN tipo = 'inicial' THEN valor END) * 100 AS dif_porcentual
                FROM
                    valores; 
            """), connection, params={"fecha_inicio": fecha_inicio, "fecha_actual": fecha_actual})

            # Dolar mep
            table_usd_mep = pd.read_sql(text(f"""
                WITH valores AS (
                    SELECT 
                        venta AS valor,
                        'inicial' AS tipo
                    FROM 
                        "{schema_name}".usd_mep usd
                    INNER JOIN (
                        SELECT MAX(partition_date) AS max_partition_date 
                        FROM "{schema_name}".usd_mep
                        WHERE partition_date BETWEEN :fecha_inicio - INTERVAL '7 days' AND :fecha_inicio
                    ) part ON usd.partition_date = part.max_partition_date
                    UNION ALL
                    SELECT 
                        venta AS valor,
                        'posterior' AS tipo
                    FROM 
                        "{schema_name}".usd_mep usd
                    INNER JOIN (
                        SELECT MAX(partition_date) AS max_partition_date 
                        FROM "{schema_name}".usd_mep
                        WHERE partition_date BETWEEN :fecha_actual - INTERVAL '7 days' AND :fecha_actual
                    ) part ON usd.partition_date = part.max_partition_date
                )
                SELECT
                    MAX(CASE WHEN tipo = 'posterior' THEN valor END) as valor_actual,
                    (MAX(CASE WHEN tipo = 'posterior' THEN valor END) - 
                    MAX(CASE WHEN tipo = 'inicial' THEN valor END)) / 
                    MAX(CASE WHEN tipo = 'inicial' THEN valor END) * 100 AS dif_porcentual
                FROM
                    valores; 
            """), connection, params={"fecha_inicio": fecha_inicio, "fecha_actual": fecha_actual})

        return table_icl, table_usd_oficial, table_ipc, table_usd_blue, table_usd_mep

    except Exception as e:
        print(f"Error al extraer los datos: {e}")
        return None, None, None, None, None  # Retornar None en caso de error

    finally:
        # Liberar recursos
        if 'engine' in locals():
            engine.dispose()
