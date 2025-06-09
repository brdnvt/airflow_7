from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
import logging
import os

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2025, 6, 9),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'starbucks_elt_pipeline',
    default_args=default_args,
    description='ELT конвеєр для даних Starbucks',
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=['etl', 'starbucks', 'postgresql'],
)

def extract_data(**context):
    """
    Завдання Extract: зчитує дані з CSV файлу
    """
    try:
        csv_path = '/opt/airflow/starbucks.csv'
        
        df = pd.read_csv(csv_path)
        
        logging.info(f"Успішно зчитано {len(df)} рядків з файлу {csv_path}")
        logging.info(f"Колонки: {list(df.columns)}")
        
        temp_path = '/tmp/starbucks_temp.csv'
        df.to_csv(temp_path, index=False)
        
        return temp_path
        
    except Exception as e:
        logging.error(f"Помилка при зчитуванні даних: {str(e)}")
        raise

def load_to_database(**context):
    """
    Завдання Load: завантажує дані до PostgreSQL
    """
    try:
        temp_path = context['task_instance'].xcom_pull(task_ids='extract_data')
        
        df = pd.read_csv(temp_path)
        
        postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
        engine = postgres_hook.get_sqlalchemy_engine()
        
        df.to_sql('starbucks_raw', engine, if_exists='replace', index=False)
        
        logging.info(f"Успішно завантажено {len(df)} рядків до таблиці starbucks_raw")
        
    except Exception as e:
        logging.error(f"Помилка при завантаженні даних: {str(e)}")
        raise

def transform_high_calorie_products(**context):
    """
    Завдання Transform: фільтрує продукти з високою калорійністю
    """
    try:
        postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
        
        sql_query = """
        DROP TABLE IF EXISTS high_calorie_products;
        CREATE TABLE high_calorie_products AS
        SELECT 
            product_name,
            size,
            calories,
            total_fat_g,
            sugar_g,
            caffeine_mg
        FROM starbucks_raw 
        WHERE calories > 300
        ORDER BY calories DESC;
        """
        
        postgres_hook.run(sql_query)
        
        count_query = "SELECT COUNT(*) FROM high_calorie_products;"
        result = postgres_hook.get_first(count_query)
        
        logging.info(f"Створено таблицю high_calorie_products з {result[0]} записами")
        
    except Exception as e:
        logging.error(f"Помилка при трансформації даних (висококолорійні продукти): {str(e)}")
        raise

def transform_aggregated_stats(**context):
    """
    Завдання Transform: створює агреговану статистику по розмірах
    """
    try:
        postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
        
        sql_query = """
        DROP TABLE IF EXISTS size_statistics;
        CREATE TABLE size_statistics AS
        SELECT 
            size,
            COUNT(*) as product_count,
            ROUND(AVG(calories)::numeric, 2) as avg_calories,
            ROUND(AVG(total_fat_g)::numeric, 2) as avg_fat,
            ROUND(AVG(sugar_g)::numeric, 2) as avg_sugar,
            ROUND(AVG(caffeine_mg)::numeric, 2) as avg_caffeine,
            MAX(calories) as max_calories,
            MIN(calories) as min_calories
        FROM starbucks_raw 
        GROUP BY size
        ORDER BY avg_calories DESC;
        """
        
        postgres_hook.run(sql_query)
        
        result_query = "SELECT * FROM size_statistics;"
        results = postgres_hook.get_records(result_query)
        
        logging.info(f"Створено таблицю size_statistics з {len(results)} записами")
        for row in results:
            logging.info(f"Розмір: {row[0]}, Середні калорії: {row[2]}")
        
    except Exception as e:
        logging.error(f"Помилка при створенні агрегованої статистики: {str(e)}")
        raise

def transform_caffeine_categories(**context):
    """
    Завдання Transform: категоризує продукти за вмістом кофеїну
    """
    try:
        postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
        
        sql_query = """
        DROP TABLE IF EXISTS caffeine_categories;
        CREATE TABLE caffeine_categories AS
        SELECT 
            product_name,
            size,
            caffeine_mg,
            CASE 
                WHEN caffeine_mg = 0 THEN 'Без кофеїну'
                WHEN caffeine_mg > 0 AND caffeine_mg <= 50 THEN 'Низький вміст'
                WHEN caffeine_mg > 50 AND caffeine_mg <= 150 THEN 'Середній вміст'
                WHEN caffeine_mg > 150 AND caffeine_mg <= 300 THEN 'Високий вміст'
                ELSE 'Дуже високий вміст'            END as caffeine_category,
            calories,
            total_fat_g
        FROM starbucks_raw 
        ORDER BY caffeine_mg DESC;
        """
        
        postgres_hook.run(sql_query)
        
        stats_query = """
        SELECT 
            caffeine_category, 
            COUNT(*) as count,
            ROUND(AVG(caffeine_mg)::numeric, 2) as avg_caffeine
        FROM caffeine_categories 
        GROUP BY caffeine_category 
        ORDER BY avg_caffeine DESC;
        """
        
        results = postgres_hook.get_records(stats_query)
        
        logging.info("Статистика по категоріях кофеїну:")
        for row in results:
            logging.info(f"Категорія: {row[0]}, Кількість: {row[1]}, Середній вміст: {row[2]} мг")
        
    except Exception as e:
        logging.error(f"Помилка при категоризації за кофеїном: {str(e)}")
        raise

create_table_task = PostgresOperator(
    task_id='create_tables',
    postgres_conn_id='postgres_default',
    sql="""
    CREATE TABLE IF NOT EXISTS starbucks_raw (
        product_name VARCHAR(255),
        size VARCHAR(50),
        milk INTEGER,
        whip INTEGER,
        serv_size_m_l INTEGER,
        calories INTEGER,
        total_fat_g DECIMAL(5,2),
        saturated_fat_g DECIMAL(5,2),
        trans_fat_g DECIMAL(5,2),
        cholesterol_mg INTEGER,
        sodium_mg INTEGER,
        total_carbs_g INTEGER,
        fiber_g INTEGER,
        sugar_g INTEGER,
        caffeine_mg INTEGER
    );
    """,
    dag=dag,
)

extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_to_database',
    python_callable=load_to_database,
    dag=dag,
)

transform_high_calorie_task = PythonOperator(
    task_id='transform_high_calorie_products',
    python_callable=transform_high_calorie_products,
    dag=dag,
)

transform_stats_task = PythonOperator(
    task_id='transform_aggregated_stats',
    python_callable=transform_aggregated_stats,
    dag=dag,
)

transform_caffeine_task = PythonOperator(
    task_id='transform_caffeine_categories',
    python_callable=transform_caffeine_categories,
    dag=dag,
)

create_table_task >> extract_task >> load_task >> [
    transform_high_calorie_task,
    transform_stats_task,
    transform_caffeine_task
]
