from __future__ import annotations

import logging
from datetime import timedelta

import pendulum
from airflow.decorators import dag, task
from airflow.exceptions import AirflowException
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

# IDs de conexión de Airflow.
API_CONN_ID = "open_meteo_api"
POSTGRES_CONN_ID = "postgres_default"

# Parámetros para la API: Latitud y Longitud de Bogotá, Colombia
LATITUDE = 4.7110
LONGITUDE = -74.0721

# --- Argumentos por Defecto del DAG ---
default_args = {
    "owner": "airflow",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
}


# --- Definición del DAG ---
@dag(
    dag_id="weather_pipeline",
    default_args=default_args,
    description="Un pipeline ETL que extrae datos del clima y los carga en PostgreSQL.",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule_interval="@daily",
    catchup=False,
    tags=["weather", "etl", "api", "example"],
    doc_md="""
    ### Pipeline ETL de Datos del Clima

    Este DAG extrae los datos del clima actuales para una ubicación específica (Bogotá) desde la API de Open-Meteo,
    los transforma a un formato limpio y los carga en una tabla de PostgreSQL.

    **Pasos:**
    1.  **create_weather_table**: Asegura que la tabla de destino exista en la base de datos (operación idempotente).
    2.  **extract_weather_data**: Obtiene los datos del clima de la API.
    3.  **transform_weather_data**: Valida y limpia los datos JSON recibidos.
    4.  **load_weather_data**: Inserta los datos transformados en la tabla de PostgreSQL.
    """,
)
#    Define el flujo de trabajo del pipeline ETL para los datos del clima.
def weather_etl_pipeline():

    @task()
    def create_weather_table():
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        create_table_sql = """
            CREATE TABLE IF NOT EXISTS weather_data (
                id SERIAL PRIMARY KEY,
                latitude REAL NOT NULL,
                longitude REAL NOT NULL,
                temperature REAL,
                windspeed REAL,
                winddirection INTEGER,
                weathercode INTEGER,
                extraction_time TIMESTAMPTZ,
                insertion_time TIMESTAMPTZ DEFAULT NOW()
            );
        """
        pg_hook.run(create_table_sql)
        logging.info("Tabla 'weather_data' verificada y lista para recibir datos.")

# Extrae los datos actuales del clima desde la API de Open-Meteo. - Realiza una validación básica de la respuesta.
    @task()
    def extract_weather_data() -> dict:
        logging.info(f"Extrayendo datos del clima para Lat: {LATITUDE}, Lon: {LONGITUDE}")
        http_hook = HttpHook(http_conn_id=API_CONN_ID, method="GET")
        endpoint = "v1/forecast"
        params = {"latitude": LATITUDE, "longitude": LONGITUDE, "current_weather": "true"}

        # http_hook.run levanta una AirflowException en caso de errores HTTP
        response = http_hook.run(endpoint, data=params)
        data = response.json()

        if "current_weather" not in data or not isinstance(data["current_weather"], dict):
            raise AirflowException(
                "Formato de datos inválido desde la API: la clave 'current_weather' no existe o no es un diccionario."
            )

        logging.info("Extracción de datos realizada con éxito.")
        return data["current_weather"]

# Valida y transforma los datos crudos del clima a un formato estructurado.
    @task()
    def transform_weather_data(raw_data: dict) -> dict:
        logging.info(f"Transformando datos crudos: {raw_data}")

        required_keys = ["temperature", "windspeed", "winddirection", "weathercode", "time"]
        if not all(key in raw_data for key in required_keys):
            raise AirflowException(
                f"Faltan claves requeridas en los datos. Se esperaban: {required_keys}"
            )

        transformed_data = {
            "latitude": LATITUDE,
            "longitude": LONGITUDE,
            "temperature": raw_data["temperature"],
            "windspeed": raw_data["windspeed"],
            "winddirection": raw_data["winddirection"],
            "weathercode": raw_data["weathercode"],
            "extraction_time": raw_data["time"],
        }
        logging.info(f"Datos transformados: {transformed_data}")
        return transformed_data

# Carga los datos del clima transformados en la base de datos PostgreSQL.
    @task()
    def load_weather_data(transformed_data: dict):
        logging.info("Iniciando la carga de datos en PostgreSQL.")
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        insert_sql = """
            INSERT INTO weather_data (
                latitude, longitude, temperature, windspeed,
                winddirection, weathercode, extraction_time
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s);
        """
        pg_hook.run(
            insert_sql,
            parameters=(
                transformed_data["latitude"],
                transformed_data["longitude"],
                transformed_data["temperature"],
                transformed_data["windspeed"],
                transformed_data["winddirection"],
                transformed_data["weathercode"],
                transformed_data["extraction_time"],
            ),
        )
        logging.info("Los datos han sido cargados exitosamente en la tabla 'weather_data'.")

    # --- Definición del Flujo de Tareas ---

    # 1. Asegurar de que la tabla exista
    table_creation_task = create_weather_table()

    # 2. Extraer datos
    raw_weather_data = extract_weather_data()

    # 3. Transformar datos
    transformed_weather_data = transform_weather_data(raw_weather_data)

    # 4. Cargar datos
    load_task = load_weather_data(transformed_weather_data)

    table_creation_task >> raw_weather_data

# --- Instanciación del DAG ---
weather_etl_pipeline()

