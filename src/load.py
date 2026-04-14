import snowflake.connector
import os
from dotenv import load_dotenv

load_dotenv()


def get_connection():
    """
    Crea y regresa una conexión a Snowflake usando variables de entorno.
    """
    required = [
        "SF_USER", "SF_PASSWORD", "SF_ACCOUNT",
        "SF_WAREHOUSE", "SF_DATABASE", "SF_SCHEMA", "SF_TABLE"
    ]

    for var in required:
        if not os.getenv(var):
            raise ValueError(f"La variable {var} no está definida en el .env")

    conn = snowflake.connector.connect(
        user=os.getenv("SF_USER"),
        password=os.getenv("SF_PASSWORD"),
        account=os.getenv("SF_ACCOUNT"),
        warehouse=os.getenv("SF_WAREHOUSE"),
        database=os.getenv("SF_DATABASE"),
        schema=os.getenv("SF_SCHEMA"),
    )

    return conn


def insert_record(cursor, record: dict) -> None:
    """
    Inserta un registro en la tabla de Snowflake.
    """
    database = os.getenv("SF_DATABASE")
    schema   = os.getenv("SF_SCHEMA")
    table    = os.getenv("SF_TABLE")

    full_table_name = f"{database}.{schema}.{table}"

    sql = f"""
        INSERT INTO {full_table_name}
        (CITY, LATITUDE, LONGITUDE, RECORDED_AT,
         TEMPERATURE_C, HUMIDITY_PCT, WIND_SPEED_KMH)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """

    cursor.execute(sql, (
        record["city"],
        record["latitude"],
        record["longitude"],
        record["recorded_at"],
        record["temperature"],
        record["humidity"],
        record["wind_speed"],
    ))


def load_all(records: list) -> None:
    """
    Inserta múltiples registros en Snowflake con manejo de errores.
    """
    if not records:
        print("No hay registros para cargar.")
        return

    conn = get_connection()
    cursor = conn.cursor()

    try:
        for record in records:
            insert_record(cursor, record)
            print(f"Insertado: {record['city']} — {record['temperature']}°C")

        conn.commit()
        print(f"\n{len(records)} registros guardados en Snowflake.")

    except Exception as e:
        conn.rollback()
        print(f"\nError al insertar, rollback ejecutado: {e}")
        raise

    finally:
        cursor.close()
        conn.close()