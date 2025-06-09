"""
Скрипт для тестування підключення до PostgreSQL
"""

import psycopg2
import pandas as pd
from sqlalchemy import create_engine
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_postgresql_connection():
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
        conn.close()
        logger.info("[УСПІХ] Базове підключення успішне!")
        
        logger.info("Тестування підключення через SQLAlchemy...")
        engine = create_engine(
            f"postgresql+psycopg2://{conn_params['user']}:{conn_params['password']}@{conn_params['host']}:{conn_params['port']}/{conn_params['database']}"
        )
        
        with engine.connect() as connection:
            result = connection.execute("SELECT 1 as test_column;")
            test_value = result.fetchone()[0]
            logger.info(f"Тестовий запит повернув: {test_value}")
        
        logger.info("[УСПІХ] SQLAlchemy підключення успішне!")
        
        logger.info("Тестування завантаження тестових даних...")
        test_data = pd.DataFrame({
            'test_column': [1, 2, 3],
            'test_value': ['a', 'b', 'c']
        })
        
        test_data.to_sql('connection_test', engine, if_exists='replace', index=False)
        
        result_df = pd.read_sql('SELECT * FROM connection_test', engine)
        logger.info(f"Завантажено {len(result_df)} рядків")
        logger.info("[УСПІХ] Тест завантаження в PostgreSQL успішний!")
        
        return True
        
    except Exception as e:
        logger.error(f"[ПОМИЛКА] Не вдалося підключитися до PostgreSQL: {str(e)}")
        return False

def test_airflow_metadata():
    """Тестування доступу до метаданих Airflow"""
    try:
        logger.info("Перевірка таблиць Airflow...")
        engine = create_engine("postgresql+psycopg2://airflow:airflow@localhost:5432/airflow")
        
        with engine.connect() as connection:
            tables_query = """
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_type = 'BASE TABLE'
            ORDER BY table_name;
            """
            
            result = connection.execute(tables_query)
            tables = [row[0] for row in result.fetchall()]
            
            logger.info(f"Знайдено {len(tables)} таблиць в базі даних")
            
            if 'dag_run' in tables:
                dag_runs_query = "SELECT COUNT(*) FROM dag_run;"
                result = connection.execute(dag_runs_query)
                dag_count = result.fetchone()[0]
                logger.info(f"Знайдено {dag_count} запусків DAG")
            
            logger.info("[УСПІХ] Доступ до метаданих Airflow успішний!")
            
        return True
        
    except Exception as e:
        logger.error(f"[ПОМИЛКА] Не вдалося перевірити метадані Airflow: {str(e)}")
        return False

if __name__ == "__main__":
    logger.info("🚀 Початок тестування підключення...")
    
    success_count = 0
    total_tests = 2
    
    if test_postgresql_connection():
        success_count += 1
    
    if test_airflow_metadata():
        success_count += 1
    
    logger.info("="*50)
    logger.info(f"📊 ПІДСУМОК ТЕСТУВАННЯ:")
    logger.info(f"   Успішних тестів: {success_count}/{total_tests}")
    
    if success_count == total_tests:
        logger.info("🎉 Всі тести пройдено успішно!")
    else:
        logger.info("⚠️  Деякі тести не пройдено")
