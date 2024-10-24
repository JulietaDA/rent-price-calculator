import os
import yaml
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
import pandas as pd

def load(ti, **context):
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

    # Definir esquema y nombres de tablas
    schema_name = f"2024_julieta_de_antonio_schema" 
    table_name = f"alquiler" 

    # Obtener el DataFrame de XCom
    # Recuperar el archivo Parquet transformado desde XCom
    transformed_parquet_file = ti.xcom_pull(task_ids='transform')
    if transformed_parquet_file is None:
        raise ValueError("No transformed data available. Aborting load.")

    # Leer el archivo Parquet transformado
    df = pd.read_parquet(transformed_parquet_file)

    partition_date = df['partition_date'].max()
    print('partition_date: ', partition_date)

    # Crear la URL de conexión 
    database_url = f"postgresql+psycopg2://{db_user}:{db_password}@{host}:{port}/{database}"

    try:
        # Crear un motor de SQLAlchemy
        engine = create_engine(database_url)

        # Verificar si el esquema existe, si no, intentamos crearlo 
        with engine.connect() as connection:
            try:
                schema_check_query = f"""
                    SELECT schema_name
                    FROM information_schema.schemata
                    WHERE schema_name = :schema_name;
                """
                result = connection.execute(text(schema_check_query), {"schema_name": schema_name})

                if not result.fetchone():
                    # Intentar crear el esquema
                    connection.execute(text(f"CREATE SCHEMA {schema_name};"))
                    print(f"Esquema '{schema_name}' creado exitosamente.")
            except Exception as e:
                # Ignorar el error si el esquema ya existe
                print(f"Esquema ya existe o error: {e}")

            # Crear una consulta SQL válida para verificar si la tabla existe en Redshift
            table_check_query = """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = :schema_name
                  AND table_name = :table_name;
            """

            # Ejecutar la consulta para verificar si la tabla existe
            result = connection.execute(text(table_check_query), {"schema_name": schema_name, "table_name": table_name})

            # Verificar si la tabla existe
            if not result.fetchone():
                # La tabla no existe, proceder con la creación de la tabla
                 connection.execute(text(f"""
                    CREATE TABLE "{schema_name}"."{table_name}" (
                        alquiler_inicial INTEGER,         -- Pago inicial de alquiler
                        alquiler_hoy INTEGER,             -- Alquiler ajustado actual
                        porc_icl DECIMAL,                 -- Diferencia porcentual del ICL
                        ipc_acum DECIMAL,                 -- IPC acumulado
                        usd_ofic DECIMAL,                 -- Valor actual del USD oficial
                        porc_ofic DECIMAL,                -- Diferencia porcentual del USD oficial
                        usd_blue DECIMAL,                 -- Valor actual del USD blue
                        porc_blue DECIMAL,                -- Diferencia porcentual del USD blue
                        usd_mep DECIMAL,                  -- Valor actual del USD MEP
                        porc_mep DECIMAL,                 -- Diferencia porcentual del USD MEP
                        alquiler_usd_ofic INTEGER,        -- Alquiler en USD oficial
                        alquiler_usd_blue INTEGER,        -- Alquiler en USD blue
                        alquiler_usd_mep INTEGER,         -- Alquiler en USD MEP
                        partition_date DATE               -- Fecha de partición
                    );
                """))                
            print(f"Tabla '{table_name}' creada exitosamente.")

            # Eliminar datos existentes para la fecha de partición --> busco que se pueda reejecutar el dag y no haya varios registros para el mismo dia
            delete_query = f"""
                DELETE FROM "{schema_name}"."{table_name}"
                WHERE partition_date = :partition_date;
            """
            connection.execute(text(delete_query), {"partition_date": partition_date})
            print(f"Datos anteriores para la fecha {partition_date} eliminados.")

        # Insertar nuevos datos en la tabla 
        with engine.connect() as conn:
            df.to_sql(
                table_name,
                con=conn,  
                schema=schema_name,
                if_exists='append',  # Agregar nuevos datos
                index=False  # No escribir el índice del DataFrame como una columna
            )
            print("Datos insertados exitosamente.")

    except Exception as e:
        print(f"Error: {e}")
    finally:
        # Liberar recursos
        engine.dispose()