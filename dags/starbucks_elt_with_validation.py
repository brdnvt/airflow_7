from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
import logging

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2025, 6, 9),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag_with_validation = DAG(
    'starbucks_elt_with_validation',
    default_args=default_args,
    description='ELT конвеєр для даних Starbucks з перевіркою якості даних',
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=['etl', 'starbucks', 'postgresql', 'data-quality'],
)

class DataValidationError(Exception):
    """Виключення для помилок валідації даних"""
    pass

def extract_data(**context):
    """Завдання Extract: зчитує дані з CSV файлу"""
    try:
        csv_path = '/opt/airflow/starbucks.csv'
        df = pd.read_csv(csv_path)
        
        logging.info(f"Успішно зчитано {len(df)} рядків з файлу {csv_path}")
        
        if df.empty:
            raise DataValidationError("CSV файл порожній!")
        
        temp_path = '/tmp/starbucks_temp_validated.csv'
        df.to_csv(temp_path, index=False)
        
        return temp_path
        
    except Exception as e:
        logging.error(f"Помилка при зчитуванні даних: {str(e)}")
        raise

def load_to_database(**context):
    """Завдання Load: завантажує дані до PostgreSQL"""
    try:
        temp_path = context['task_instance'].xcom_pull(task_ids='extract_data_validated')
        df = pd.read_csv(temp_path)
        
        postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
        engine = postgres_hook.get_sqlalchemy_engine()
        
        df.to_sql('starbucks_raw_validated', engine, if_exists='replace', index=False)
        
        logging.info(f"Успішно завантажено {len(df)} рядків до таблиці starbucks_raw_validated")
        
    except Exception as e:
        logging.error(f"Помилка при завантаженні даних: {str(e)}")
        raise

def validate_data_completeness(**context):
    """
    Перевірка повноти даних
    """
    try:
        postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
        
        null_check_query = """
        SELECT 
            SUM(CASE WHEN product_name IS NULL THEN 1 ELSE 0 END) as null_product_name,
            SUM(CASE WHEN calories IS NULL THEN 1 ELSE 0 END) as null_calories,
            SUM(CASE WHEN size IS NULL THEN 1 ELSE 0 END) as null_size,
            COUNT(*) as total_records
        FROM starbucks_raw_validated;
        """
        
        result = postgres_hook.get_first(null_check_query)
        null_product_name, null_calories, null_size, total_records = result
        
        logging.info(f"Загальна кількість записів: {total_records}")
        logging.info(f"Відсутні назви продуктів: {null_product_name}")
        logging.info(f"Відсутні калорії: {null_calories}")
        logging.info(f"Відсутні розміри: {null_size}")
        
        if null_product_name > 0:
            raise DataValidationError(f"Знайдено {null_product_name} записів з відсутніми назвами продуктів!")
        
        if null_calories > 0:
            raise DataValidationError(f"Знайдено {null_calories} записів з відсутніми калоріями!")
        
        if null_size > 0:
            raise DataValidationError(f"Знайдено {null_size} записів з відсутніми розмірами!")
        
        if total_records < 100:
            raise DataValidationError(f"Недостатньо даних: {total_records} записів (очікувалось мінімум 100)")
        
        logging.info("✅ Тест повноти даних пройдено успішно!")
        
    except DataValidationError:
        raise
    except Exception as e:
        logging.error(f"Помилка при перевірці повноти даних: {str(e)}")
        raise DataValidationError(f"Помилка валідації: {str(e)}")

def validate_data_ranges(**context):
    """
    Перевірка діапазонів значень
    """
    try:
        postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
        
        calorie_range_query = """
        SELECT 
            MIN(calories) as min_calories,
            MAX(calories) as max_calories,
            AVG(calories) as avg_calories,
            COUNT(*) as total_records,
            SUM(CASE WHEN calories < 0 THEN 1 ELSE 0 END) as negative_calories,
            SUM(CASE WHEN calories > 1000 THEN 1 ELSE 0 END) as extreme_calories
        FROM starbucks_raw_validated;
        """
        
        result = postgres_hook.get_first(calorie_range_query)
        min_cal, max_cal, avg_cal, total, negative_cal, extreme_cal = result
        
        logging.info(f"Діапазон калорій: {min_cal} - {max_cal}, середнє: {avg_cal:.2f}")
        
        if negative_cal > 0:
            raise DataValidationError(f"Знайдено {negative_cal} записів з негативними калоріями!")
        
        if extreme_cal > total * 0.1:  
            raise DataValidationError(f"Занадто багато продуктів з екстремальними калоріями: {extreme_cal}")
        
        caffeine_range_query = """
        SELECT 
            MIN(caffeine_mg) as min_caffeine,
            MAX(caffeine_mg) as max_caffeine,
            SUM(CASE WHEN caffeine_mg < 0 THEN 1 ELSE 0 END) as negative_caffeine,
            SUM(CASE WHEN caffeine_mg > 500 THEN 1 ELSE 0 END) as extreme_caffeine
        FROM starbucks_raw_validated;
        """
        
        result = postgres_hook.get_first(caffeine_range_query)
        min_caff, max_caff, negative_caff, extreme_caff = result
        
        logging.info(f"Діапазон кофеїну: {min_caff} - {max_caff} мг")
        
        if negative_caff > 0:
            raise DataValidationError(f"Знайдено {negative_caff} записів з негативним вмістом кофеїну!")
        
        if extreme_caff > 0:
            logging.warning(f"Знайдено {extreme_caff} продуктів з дуже високим вмістом кофеїну (>500мг)")
        
        logging.info("✅ Тест діапазонів значень пройдено успішно!")
        
    except DataValidationError:
        raise
    except Exception as e:
        logging.error(f"Помилка при перевірці діапазонів: {str(e)}")
        raise DataValidationError(f"Помилка валідації діапазонів: {str(e)}")

def validate_business_rules(**context):
    """
    Перевірка бізнес-правил
    """
    try:
        postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
        
        size_calorie_query = """
        SELECT 
            size,
            AVG(calories) as avg_calories,
            COUNT(*) as count
        FROM starbucks_raw_validated        WHERE size IN ('short', 'tall', 'grande', 'venti')
        GROUP BY size
        ORDER BY 
            CASE size 
                WHEN 'short' THEN 1
                WHEN 'tall' THEN 2  
                WHEN 'grande' THEN 3
                WHEN 'venti' THEN 4
            END;
        """
        
        results = postgres_hook.get_records(size_calorie_query)
        
        if len(results) >= 2:
            prev_calories = 0
            for size, avg_calories, count in results:
                logging.info(f"Розмір {size}: середні калорії {avg_calories:.2f} ({count} продуктів)")
                
                if prev_calories > 0 and float(avg_calories) < float(prev_calories) * 0.8:
                    logging.warning(f"Можлива аномалія: {size} має менше калорій ніж попередній розмір")
                
                prev_calories = avg_calories
        
        fat_calorie_query = """
        SELECT 
            COUNT(*) as inconsistent_records
        FROM starbucks_raw_validated 
        WHERE total_fat_g > 50 AND calories < 100;
        """
        
        result = postgres_hook.get_first(fat_calorie_query)
        inconsistent_records = result[0]
        
        if inconsistent_records > 0:
            logging.warning(f"Знайдено {inconsistent_records} записів з високим вмістом жиру та низькими калоріями")
        
        size_variety_query = """
        SELECT DISTINCT size 
        FROM starbucks_raw_validated 
        ORDER BY size;
        """
        
        sizes = [row[0] for row in postgres_hook.get_records(size_variety_query)]
        expected_sizes = ['short', 'tall', 'grande', 'venti']
        
        logging.info(f"Знайдені розміри: {sizes}")
        
        missing_sizes = [size for size in expected_sizes if size not in sizes]
        if missing_sizes:
            logging.warning(f"Відсутні стандартні розміри: {missing_sizes}")
        
        logging.info("✅ Тест бізнес-правил пройдено успішно!")
        
    except Exception as e:
        logging.error(f"Помилка при перевірці бізнес-правил: {str(e)}")
        raise DataValidationError(f"Помилка валідації бізнес-правил: {str(e)}")

def transform_with_validation(**context):
    """
    Трансформація даних з додатковими перевірками
    """
    try:
        postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
        
        sql_query = """
        DROP TABLE IF EXISTS starbucks_summary_validated;
        CREATE TABLE starbucks_summary_validated AS
        SELECT 
            product_name,
            size,
            calories,
            total_fat_g,
            sugar_g,
            caffeine_mg,
            CASE 
                WHEN calories <= 100 THEN 'Низькокалорійний'
                WHEN calories BETWEEN 101 AND 300 THEN 'Середньокалорійний'
                ELSE 'Висококалорійний'
            END as calorie_category,
            CASE 
                WHEN caffeine_mg = 0 THEN 'Без кофеїну'
                WHEN caffeine_mg <= 150 THEN 'Помірний кофеїн'
                ELSE 'Високий кофеїн'
            END as caffeine_category
        FROM starbucks_raw_validated
        WHERE calories >= 0 AND total_fat_g >= 0 AND caffeine_mg >= 0;
        """
        
        postgres_hook.run(sql_query)
        
        validation_query = """
        SELECT 
            COUNT(*) as total_transformed,
            COUNT(DISTINCT product_name) as unique_products,
            COUNT(DISTINCT calorie_category) as calorie_categories,
            COUNT(DISTINCT caffeine_category) as caffeine_categories
        FROM starbucks_summary_validated;
        """
        
        result = postgres_hook.get_first(validation_query)
        total_transformed, unique_products, calorie_cat, caffeine_cat = result
        
        logging.info(f"Трансформовано записів: {total_transformed}")
        logging.info(f"Унікальних продуктів: {unique_products}")
        logging.info(f"Категорій калорій: {calorie_cat}")
        logging.info(f"Категорій кофеїну: {caffeine_cat}")
        
        if total_transformed == 0:
            raise DataValidationError("Трансформація не створила жодного запису!")
        
        if calorie_cat != 3:
            raise DataValidationError(f"Очікувалось 3 категорії калорій, отримано {calorie_cat}")
        
        if caffeine_cat != 3:
            raise DataValidationError(f"Очікувалось 3 категорії кофеїну, отримано {caffeine_cat}")
        
        logging.info("✅ Трансформація з валідацією завершена успішно!")
        
    except DataValidationError:
        raise
    except Exception as e:
        logging.error(f"Помилка при трансформації з валідацією: {str(e)}")
        raise DataValidationError(f"Помилка трансформації: {str(e)}")

create_table_validated = PostgresOperator(
    task_id='create_tables_validated',
    postgres_conn_id='postgres_default',
    sql="""
    CREATE TABLE IF NOT EXISTS starbucks_raw_validated (
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
    dag=dag_with_validation,
)

extract_validated = PythonOperator(
    task_id='extract_data_validated',
    python_callable=extract_data,
    dag=dag_with_validation,
)

load_validated = PythonOperator(
    task_id='load_to_database_validated',
    python_callable=load_to_database,
    dag=dag_with_validation,
)

validate_completeness = PythonOperator(
    task_id='validate_data_completeness',
    python_callable=validate_data_completeness,
    dag=dag_with_validation,
)

validate_ranges = PythonOperator(
    task_id='validate_data_ranges',  
    python_callable=validate_data_ranges,
    dag=dag_with_validation,
)

validate_business = PythonOperator(
    task_id='validate_business_rules',
    python_callable=validate_business_rules,
    dag=dag_with_validation,
)

transform_validated = PythonOperator(
    task_id='transform_with_validation',
    python_callable=transform_with_validation,
    dag=dag_with_validation,
)

create_table_validated >> extract_validated >> load_validated >> [
    validate_completeness,
    validate_ranges,
    validate_business
] >> transform_validated
