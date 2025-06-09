
POSTGRES_HOST = "localhost"
POSTGRES_PORT = "5432"
POSTGRES_DB = "airflow"
POSTGRES_USER = "airflow"
POSTGRES_PASSWORD = "airflow"

AIRFLOW_DB_CONNECTION = f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

AIRFLOW_HOME = "C:/Users/bardi/Downloads/airflow_6"
DAGS_FOLDER = "C:/Users/bardi/Downloads/airflow_6/dags"
LOGS_FOLDER = "C:/Users/bardi/Downloads/airflow_6/logs"
