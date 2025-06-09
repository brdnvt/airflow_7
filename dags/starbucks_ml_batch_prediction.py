from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
import numpy as np
import logging
import joblib
from sklearn.preprocessing import StandardScaler, LabelEncoder

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
    'starbucks_ml_batch_prediction',
    default_args=default_args,
    description='Batch прогнозування з використанням навчених ML моделей',
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=['ml', 'prediction', 'batch', 'starbucks'],
)

def get_latest_models(**context):
    """
    Отримання інформації про найновіші навчені моделі
    """
    try:
        postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
        
        regression_query = """
        SELECT model_id, model_name, performance_metric, model_path, feature_columns
        FROM ml_models_metadata 
        WHERE model_type = 'regression'
        ORDER BY created_at DESC 
        LIMIT 1;
        """
        
        regression_result = postgres_hook.get_first(regression_query)
        
        classification_query = """
        SELECT model_id, model_name, performance_metric, model_path, feature_columns
        FROM ml_models_metadata 
        WHERE model_type = 'classification'
        ORDER BY created_at DESC 
        LIMIT 1;
        """
        
        classification_result = postgres_hook.get_first(classification_query)
        
        if not regression_result or not classification_result:
            raise ValueError("Не знайдено навчених моделей в базі даних")
        
        models_info = {
            'regression': {
                'model_id': regression_result[0],
                'model_name': regression_result[1],
                'performance': regression_result[2],
                'model_path': regression_result[3],
                'features': regression_result[4].split(',')
            },
            'classification': {
                'model_id': classification_result[0],
                'model_name': classification_result[1],
                'performance': classification_result[2],
                'model_path': classification_result[3],
                'features': classification_result[4].split(',')
            }
        }
        
        logging.info(f"Знайдено моделі:")
        logging.info(f"  Регресія: {models_info['regression']['model_id']} (R²={models_info['regression']['performance']})")
        logging.info(f"  Класифікація: {models_info['classification']['model_id']} (Acc={models_info['classification']['performance']})")
        
        return models_info
        
    except Exception as e:
        logging.error(f"Помилка при отриманні інформації про моделі: {str(e)}")
        raise

def prepare_new_data(**context):
    """
    Підготовка нових даних для прогнозування
    """
    try:
        postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
        
        test_data = {
            'product_name': [
                'Custom Latte - Medium',
                'Custom Frappuccino - Large', 
                'Custom Americano - Small',
                'Custom Mocha - Grande',
                'Custom Tea - Tall'
            ],
            'size': ['grande', 'venti', 'short', 'grande', 'tall'],
            'milk': [1, 1, 0, 1, 0],
            'whip': [0, 1, 0, 1, 0],
            'serv_size_m_l': [473, 591, 236, 473, 354],
            'total_fat_g': [3.5, 15.0, 0.1, 8.0, 0.0],
            'sugar_g': [25, 45, 0, 35, 20],
            'caffeine_mg': [150, 95, 225, 175, 40]
        }
        
        df_new = pd.DataFrame(test_data)
        
        df_new['calories'] = (
            df_new['serv_size_m_l'] * 0.3 + 
            df_new['total_fat_g'] * 9 + 
            df_new['sugar_g'] * 4 +
            df_new['milk'] * 50 +
            df_new['whip'] * 100
        ).astype(int)
        
        le_size = joblib.load('/tmp/size_encoder.joblib')
        df_new['size_encoded'] = le_size.transform(df_new['size'])
        
        temp_path = '/tmp/new_data_for_prediction.csv'
        df_new.to_csv(temp_path, index=False)
        
        logging.info(f"Підготовлено {len(df_new)} нових записів для прогнозування")
        logging.info(f"Продукти: {df_new['product_name'].tolist()}")
        
        return {
            'data_path': temp_path,
            'records_count': len(df_new)
        }
        
    except Exception as e:
        logging.error(f"Помилка при підготовці нових даних: {str(e)}")
        raise

def batch_predict(**context):
    """
    Виконання batch прогнозування
    """
    try:
        models_info = context['task_instance'].xcom_pull(task_ids='get_latest_models')
        data_info = context['task_instance'].xcom_pull(task_ids='prepare_new_data')
        
        df = pd.read_csv(data_info['data_path'])
        
        regression_model = joblib.load(models_info['regression']['model_path'])
        classification_model = joblib.load(models_info['classification']['model_path'])
        
        try:
            regression_scaler = joblib.load('/tmp/regression_scaler.joblib')
            classification_scaler = joblib.load('/tmp/classification_scaler.joblib')
        except:
            regression_scaler = None
            classification_scaler = None
            logging.warning("Скейлери не знайдено, використовуємо сирі дані")
        
        regression_features = df[models_info['regression']['features']]
        
        if models_info['regression']['model_name'] == 'linear_regression' and regression_scaler:
            regression_features_scaled = regression_scaler.transform(regression_features)
            predicted_calories = regression_model.predict(regression_features_scaled)
        else:
            predicted_calories = regression_model.predict(regression_features)
        
        classification_features = df[models_info['classification']['features']]
        
        if models_info['classification']['model_name'] == 'logistic_regression' and classification_scaler:
            classification_features_scaled = classification_scaler.transform(classification_features)
            predicted_caffeine_category = classification_model.predict(classification_features_scaled)
        else:
            predicted_caffeine_category = classification_model.predict(classification_features)
        
        results_df = df[['product_name', 'size', 'calories', 'caffeine_mg']].copy()
        results_df['predicted_calories'] = predicted_calories.round().astype(int)
        results_df['predicted_caffeine_category'] = predicted_caffeine_category
        results_df['prediction_timestamp'] = datetime.now()
        results_df['regression_model_id'] = models_info['regression']['model_id']
        results_df['classification_model_id'] = models_info['classification']['model_id']
        
        postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
        engine = postgres_hook.get_sqlalchemy_engine()
        
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS batch_predictions (
            id SERIAL PRIMARY KEY,
            product_name VARCHAR(255),
            size VARCHAR(50),
            actual_calories INTEGER,
            actual_caffeine_mg INTEGER,
            predicted_calories INTEGER,
            predicted_caffeine_category VARCHAR(50),
            prediction_timestamp TIMESTAMP,
            regression_model_id VARCHAR(255),
            classification_model_id VARCHAR(255)
        );
        """
        
        postgres_hook.run(create_table_sql)
        
        results_df.rename(columns={
            'calories': 'actual_calories',
            'caffeine_mg': 'actual_caffeine_mg'
        }).to_sql('batch_predictions', engine, if_exists='append', index=False)
        
        logging.info("=== РЕЗУЛЬТАТИ BATCH ПРОГНОЗУВАННЯ ===")
        for idx, row in results_df.iterrows():
            logging.info(f"Продукт: {row['product_name']}")
            logging.info(f"  Розмір: {row['size']}")
            logging.info(f"  Фактичні калорії: {row['calories']}")
            logging.info(f"  Прогнозовані калорії: {row['predicted_calories']}")
            logging.info(f"  Прогнозована категорія кофеїну: {row['predicted_caffeine_category']}")
            logging.info(f"  Використані моделі: {row['regression_model_id'][:30]}... / {row['classification_model_id'][:30]}...")
            logging.info("---")
        
        return {
            'predictions_made': len(results_df),
            'regression_model_used': models_info['regression']['model_id'],
            'classification_model_used': models_info['classification']['model_id'],
            'prediction_timestamp': datetime.now().isoformat()
        }
        
    except Exception as e:
        logging.error(f"Помилка при batch прогнозуванні: {str(e)}")
        raise

def generate_prediction_report(**context):
    """
    Генерація звіту по прогнозуванню
    """
    try:
        prediction_results = context['task_instance'].xcom_pull(task_ids='batch_predict')
        
        postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
        
        stats_query = """
        SELECT 
            COUNT(*) as total_predictions,
            AVG(ABS(actual_calories - predicted_calories)) as avg_calories_error,
            predicted_caffeine_category,
            COUNT(*) as category_count
        FROM batch_predictions 
        WHERE prediction_timestamp >= CURRENT_DATE
        GROUP BY predicted_caffeine_category
        ORDER BY category_count DESC;
        """
        
        stats_results = postgres_hook.get_records(stats_query)
        
        logging.info("=== ЗВІТ ПО ПРОГНОЗУВАННЮ ===")
        logging.info(f"Час виконання: {prediction_results['prediction_timestamp']}")
        logging.info(f"Кількість прогнозів: {prediction_results['predictions_made']}")
        logging.info(f"Регресійна модель: {prediction_results['regression_model_used']}")
        logging.info(f"Класифікаційна модель: {prediction_results['classification_model_used']}")
        
        logging.info("\nРозподіл по категоріях кофеїну:")
        for row in stats_results:
            logging.info(f"  {row[2]}: {row[3]} прогнозів")
        
        metrics_sql = """
        CREATE TABLE IF NOT EXISTS prediction_metrics (
            id SERIAL PRIMARY KEY,
            report_date DATE DEFAULT CURRENT_DATE,
            total_predictions INTEGER,
            regression_model_id VARCHAR(255),
            classification_model_id VARCHAR(255),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        INSERT INTO prediction_metrics (total_predictions, regression_model_id, classification_model_id)
        VALUES (%s, %s, %s);
        """
        
        postgres_hook.run(metrics_sql, parameters=(
            prediction_results['predictions_made'],
            prediction_results['regression_model_used'],
            prediction_results['classification_model_used']
        ))
        
        logging.info("Звіт збережено в базу даних")
        
        return {
            'report_generated': True,
            'total_predictions': prediction_results['predictions_made']
        }
        
    except Exception as e:
        logging.error(f"Помилка при генерації звіту: {str(e)}")
        raise

get_models_task = PythonOperator(
    task_id='get_latest_models',
    python_callable=get_latest_models,
    dag=dag,
)

prepare_data_task = PythonOperator(
    task_id='prepare_new_data',
    python_callable=prepare_new_data,
    dag=dag,
)

batch_predict_task = PythonOperator(
    task_id='batch_predict',
    python_callable=batch_predict,
    dag=dag,
)

generate_report_task = PythonOperator(
    task_id='generate_prediction_report',
    python_callable=generate_prediction_report,
    dag=dag,
)

[get_models_task, prepare_data_task] >> batch_predict_task >> generate_report_task
