"""
Скрипт для перевірки підключення до PostgreSQL та тестування основних операцій
"""

import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_postgres_connection():
    """Тестування підключення до PostgreSQL"""
    
    conn_params = {
        'host': 'localhost',
        'port': '5432',
        'database': 'airflow',
        'user': 'airflow',
        'password': 'airflow'
    }
    
    try:
        logger.info("Тестування підключення до PostgreSQL...")
        conn = psycopg2.connect(**conn_params)
        cursor = conn.cursor()
        
        cursor.execute("SELECT version();")
        version = cursor.fetchone()[0]
        logger.info(f"PostgreSQL версія: {version}")
        
        cursor.close()
        conn.close()        logger.info("[УСПІХ] Базове підключення успішне!")
        
        logger.info("Тестування підключення через SQLAlchemy...")
        engine = create_engine(
            f"postgresql+psycopg2://{conn_params['user']}:{conn_params['password']}@{conn_params['host']}:{conn_params['port']}/{conn_params['database']}"
        )
        
        with engine.connect() as connection:
            result = connection.execute("SELECT 1 as test_column;")
            test_value = result.fetchone()[0]
            logger.info(f"Тестовий запит повернув: {test_value}")
          logger.info("[УСПІХ] SQLAlchemy підключення успішне!")
        
        logger.info("Тестування завантаження CSV файлу...")
        csv_path = r'c:\Users\bardi\Downloads\airflow_6\starbucks.csv'
        df = pd.read_csv(csv_path)
        
        logger.info(f"Завантажено {len(df)} рядків з CSV")
        logger.info(f"Колонки: {list(df.columns)}")
        logger.info(f"Перші 3 рядки:\n{df.head(3)}")
        
        logger.info("Тестування збереження даних в PostgreSQL...")
        df.head(10).to_sql('test_starbucks', engine, if_exists='replace', index=False)
        
        with engine.connect() as connection:
            result = connection.execute("SELECT COUNT(*) FROM test_starbucks;")
            count = result.fetchone()[0]
            logger.info(f"Збережено {count} тестових записів")
        
        with engine.connect() as connection:
            connection.execute("DROP TABLE IF EXISTS test_starbucks;")
          logger.info("[УСПІХ] Тест завантаження в PostgreSQL успішний!")
        
        return True
        
    except Exception as e:
        logger.error(f"[ПОМИЛКА] Помилка при тестуванні: {str(e)}")
        return False

def check_airflow_tables():
    """Перевірка наявності таблиць Airflow"""
    
    conn_params = {
        'host': 'localhost',
        'port': '5432',
        'database': 'airflow',
        'user': 'airflow',
        'password': 'airflow'
    }
    
    try:
        engine = create_engine(
            f"postgresql+psycopg2://{conn_params['user']}:{conn_params['password']}@{conn_params['host']}:{conn_params['port']}/{conn_params['database']}"
        )
        
        with engine.connect() as connection:
            result = connection.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name LIKE '%dag%' OR table_name LIKE '%task%'
                ORDER BY table_name;
            """)
            
            tables = [row[0] for row in result.fetchall()]
              if tables:
                logger.info("[ЗНАЙДЕНО] Знайдені таблиці Airflow:")
                for table in tables:
                    logger.info(f"  - {table}")
            else:
                logger.warning("[УВАГА] Таблиці Airflow не знайдені. Можливо потрібно запустити 'airflow db init'")
                  except Exception as e:
        logger.error(f"[ПОМИЛКА] Помилка при перевірці таблиць Airflow: {str(e)}")

if __name__ == "__main__":
    logger.info("Початок тестування підключення PostgreSQL...")
    
    if test_postgres_connection():
        logger.info("\n" + "="*50)
        logger.info("Перевірка таблиць Airflow...")
        check_airflow_tables()
          logger.info("\n" + "="*50)
        logger.info("[УСПІХ] Всі тести пройдені успішно!")
        logger.info("Тепер ви можете запускати Airflow DAGs")
    else:
        logger.error("[ПОМИЛКА] Є проблеми з підключенням. Перевірте налаштування PostgreSQL")
