import pandas as pd
from airflow.hooks.postgres_hook import PostgresHook
import os
import yaml
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

def load_data_to_redshift(table_sufix, ti, **context):
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
    db_user = os.getenv('DB_USER')  # Información sensible desde variables de entorno
    db_password = os.getenv('DB_PASSWORD')  # Información sensible desde variables de entorno

    # Definir esquema y nombres de tablas
    schema_name = f"{db_user}_schema"  # Cambia esto al nombre de esquema deseado
    table_name = f"usd_{table_sufix}"  # Reemplaza con el nombre real de la tabla
    print(f"Table name: {table_name}")

    # Obtener el DataFrame de XCom
    df = ti.xcom_pull(task_ids='transform_data')
    if df is None:  # Verificar si la extracción fue exitosa
        raise ValueError("No se extrajeron datos. Abortando la transformación.")

    partition_date = df['partition_date'].unique()[0]
    print('partition_date: ', partition_date)

    # Crear la URL de conexión para SQLAlchemy (compatible con Redshift)
    database_url = f"postgresql+psycopg2://{db_user}:{db_password}@{host}:{port}/{database}"

    try:
        # Crear un motor de SQLAlchemy
        engine = create_engine(database_url)

        # Verificar si el esquema existe, si no, intentamos crearlo (Redshift no soporta IF NOT EXISTS)
        with engine.connect() as connection:
            try:
                schema_check_query = f"""
                    SELECT schema_name
                    FROM information_schema.schemata
                    WHERE schema_name = :schema_name;
                """
                result = connection.execute(text(schema_check_query), {"schema_name": schema_name})

                if not result.fetchone():
                    # Intentar crear el esquema. Redshift no tiene IF NOT EXISTS, así que ignoramos errores si ya existe
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
                        casa TEXT,
                        compra INTEGER,
                        venta INTEGER,
                        partition_date_usd DATE,
                        partition_date DATE
                    );
                """))
                print(f"Tabla '{table_name}' creada exitosamente.")

            # Eliminar datos existentes para la fecha de partición
            delete_query = f"""
                DELETE FROM "{schema_name}"."{table_name}"
                WHERE partition_date = :partition_date;
            """
            connection.execute(text(delete_query), {"partition_date": partition_date})
            print(f"Datos anteriores para la fecha {partition_date} eliminados.")

        # Insertar nuevos datos en la tabla usando SQLAlchemy
        with engine.connect() as conn:
            df.to_sql(
                table_name,
                con=conn,  # Usar el objeto de conexión aquí
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