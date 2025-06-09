from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
import numpy as np
import logging
import joblib
import os
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor, RandomForestClassifier
from sklearn.linear_model import LinearRegression, LogisticRegression
from sklearn.metrics import mean_squared_error, r2_score, accuracy_score, classification_report, confusion_matrix
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
    'starbucks_ml_pipeline',
    default_args=default_args,
    description='ELT конвеєр з машинним навчанням для даних Starbucks',
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=['etl', 'ml', 'starbucks', 'postgresql', 'regression', 'classification'],
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
        
        temp_path = '/tmp/starbucks_ml_temp.csv'
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
        
        df.to_sql('starbucks_ml_raw', engine, if_exists='replace', index=False)
        
        logging.info(f"Успішно завантажено {len(df)} рядків до таблиці starbucks_ml_raw")
        
        return len(df)
        
    except Exception as e:
        logging.error(f"Помилка при завантаженні даних: {str(e)}")
        raise

def prepare_ml_features(**context):
    """
    Підготовка даних для машинного навчання
    """
    try:
        postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
        
        df = pd.read_sql_query("SELECT * FROM starbucks_ml_raw", postgres_hook.get_sqlalchemy_engine())
        
        df_clean = df.dropna()
        
        def categorize_caffeine(caffeine):
            if caffeine == 0:
                return 'decaf'
            elif caffeine <= 50:
                return 'low'
            elif caffeine <= 150:
                return 'medium'
            elif caffeine <= 300:
                return 'high'
            else:
                return 'very_high'
        
        df_clean['caffeine_category'] = df_clean['caffeine_mg'].apply(categorize_caffeine)
        
        le_size = LabelEncoder()
        df_clean['size_encoded'] = le_size.fit_transform(df_clean['size'])
        
        temp_ml_path = '/tmp/starbucks_ml_features.csv'
        df_clean.to_csv(temp_ml_path, index=False)
        
        encoder_path = '/tmp/size_encoder.joblib'
        joblib.dump(le_size, encoder_path)
        
        logging.info(f"Підготовлено {len(df_clean)} рядків для ML")
        logging.info(f"Розподіл категорій кофеїну: {df_clean['caffeine_category'].value_counts().to_dict()}")
        
        return {
            'data_path': temp_ml_path,
            'encoder_path': encoder_path,
            'rows_count': len(df_clean),
            'features_count': len(df_clean.columns)
        }
        
    except Exception as e:
        logging.error(f"Помилка при підготовці ML ознак: {str(e)}")
        raise

def train_regression_model(**context):
    """
    Навчання моделі регресії для прогнозування калорій
    """
    try:
        ml_info = context['task_instance'].xcom_pull(task_ids='prepare_ml_features')
        data_path = ml_info['data_path']
        
        df = pd.read_csv(data_path)
        
        feature_columns = ['size_encoded', 'milk', 'whip', 'serv_size_m_l', 
                          'total_fat_g', 'sugar_g', 'caffeine_mg']
        target_column = 'calories'
        
        X = df[feature_columns]
        y = df[target_column]
        
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, random_state=42
        )
        
        scaler = StandardScaler()
        X_train_scaled = scaler.fit_transform(X_train)
        X_test_scaled = scaler.transform(X_test)
        
        models = {
            'linear_regression': LinearRegression(),
            'random_forest': RandomForestRegressor(n_estimators=100, random_state=42)
        }
        
        best_model = None
        best_score = -float('inf')
        best_model_name = ''
        model_results = {}
        
        for name, model in models.items():
            if name == 'linear_regression':
                model.fit(X_train_scaled, y_train)
                y_pred = model.predict(X_test_scaled)
            else:
                model.fit(X_train, y_train)
                y_pred = model.predict(X_test)
            
            mse = mean_squared_error(y_test, y_pred)
            r2 = r2_score(y_test, y_pred)
            
            model_results[name] = {
                'mse': mse,
                'r2': r2,
                'rmse': np.sqrt(mse)
            }
            
            logging.info(f"Модель {name}: MSE={mse:.2f}, R2={r2:.3f}, RMSE={np.sqrt(mse):.2f}")
            
            if r2 > best_score:
                best_score = r2
                best_model = model
                best_model_name = name
        
        model_path = '/tmp/best_regression_model.joblib'
        scaler_path = '/tmp/regression_scaler.joblib'
        
        joblib.dump(best_model, model_path)
        joblib.dump(scaler, scaler_path)
        
        logging.info(f"Найкраща модель: {best_model_name} з R2={best_score:.3f}")
        
        return {
            'model_path': model_path,
            'scaler_path': scaler_path,
            'model_name': best_model_name,
            'r2_score': best_score,
            'model_results': model_results,
            'feature_columns': feature_columns,
            'model_id': f"regression_{best_model_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        }
        
    except Exception as e:
        logging.error(f"Помилка при навчанні регресійної моделі: {str(e)}")
        raise

def train_classification_model(**context):
    """
    Навчання моделі класифікації для визначення категорії кофеїну
    """
    try:
        ml_info = context['task_instance'].xcom_pull(task_ids='prepare_ml_features')
        data_path = ml_info['data_path']
        
        df = pd.read_csv(data_path)
        
        feature_columns = ['size_encoded', 'milk', 'whip', 'serv_size_m_l', 
                          'calories', 'total_fat_g', 'sugar_g']
        target_column = 'caffeine_category'
        
        X = df[feature_columns]
        y = df[target_column]
        
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, random_state=42, stratify=y
        )
        
        scaler = StandardScaler()
        X_train_scaled = scaler.fit_transform(X_train)
        X_test_scaled = scaler.transform(X_test)
        
        models = {
            'logistic_regression': LogisticRegression(random_state=42, max_iter=1000),
            'random_forest': RandomForestClassifier(n_estimators=100, random_state=42)
        }
        
        best_model = None
        best_score = -float('inf')
        best_model_name = ''
        model_results = {}
        
        for name, model in models.items():
            if name == 'logistic_regression':
                model.fit(X_train_scaled, y_train)
                y_pred = model.predict(X_test_scaled)
            else:
                model.fit(X_train, y_train)
                y_pred = model.predict(X_test)
            
            accuracy = accuracy_score(y_test, y_pred)
            
            model_results[name] = {
                'accuracy': accuracy,
                'classification_report': classification_report(y_test, y_pred, output_dict=True)
            }
            
            logging.info(f"Модель {name}: Accuracy={accuracy:.3f}")
            logging.info(f"Classification Report:\n{classification_report(y_test, y_pred)}")
            
            if accuracy > best_score:
                best_score = accuracy
                best_model = model
                best_model_name = name
        
        model_path = '/tmp/best_classification_model.joblib'
        scaler_path = '/tmp/classification_scaler.joblib'
        
        joblib.dump(best_model, model_path)
        joblib.dump(scaler, scaler_path)
        
        logging.info(f"Найкраща класифікаційна модель: {best_model_name} з Accuracy={best_score:.3f}")
        
        return {
            'model_path': model_path,
            'scaler_path': scaler_path,
            'model_name': best_model_name,
            'accuracy': best_score,
            'model_results': model_results,
            'feature_columns': feature_columns,
            'model_id': f"classification_{best_model_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        }
        
    except Exception as e:
        logging.error(f"Помилка при навчанні класифікаційної моделі: {str(e)}")
        raise

def evaluate_models(**context):
    """
    Оцінка та порівняння навчених моделей
    """
    try:
        regression_info = context['task_instance'].xcom_pull(task_ids='train_regression_model')
        classification_info = context['task_instance'].xcom_pull(task_ids='train_classification_model')
        
        logging.info("=== ОЦІНКА МОДЕЛЕЙ ===")
        
        logging.info(f"Регресійна модель:")
        logging.info(f"  ID моделі: {regression_info['model_id']}")
        logging.info(f"  Тип: {regression_info['model_name']}")
        logging.info(f"  R² Score: {regression_info['r2_score']:.3f}")
        logging.info(f"  Деталі всіх моделей: {regression_info['model_results']}")
        
        logging.info(f"Класифікаційна модель:")
        logging.info(f"  ID моделі: {classification_info['model_id']}")
        logging.info(f"  Тип: {classification_info['model_name']}")
        logging.info(f"  Accuracy: {classification_info['accuracy']:.3f}")
        
        postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
        
        create_models_table_sql = """
        CREATE TABLE IF NOT EXISTS ml_models_metadata (
            id SERIAL PRIMARY KEY,
            model_id VARCHAR(255) UNIQUE,
            model_type VARCHAR(50),
            model_name VARCHAR(100),
            performance_metric DECIMAL(8,4),
            metric_name VARCHAR(50),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            model_path VARCHAR(500),
            feature_columns TEXT
        );
        """
        
        postgres_hook.run(create_models_table_sql)
        
        insert_regression_sql = """
        INSERT INTO ml_models_metadata 
        (model_id, model_type, model_name, performance_metric, metric_name, model_path, feature_columns)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (model_id) DO UPDATE SET
        performance_metric = EXCLUDED.performance_metric,
        created_at = CURRENT_TIMESTAMP;
        """
        
        postgres_hook.run(insert_regression_sql, parameters=(
            regression_info['model_id'],
            'regression',
            regression_info['model_name'],
            regression_info['r2_score'],
            'r2_score',
            regression_info['model_path'],
            ','.join(regression_info['feature_columns'])
        ))
        
        insert_classification_sql = """
        INSERT INTO ml_models_metadata 
        (model_id, model_type, model_name, performance_metric, metric_name, model_path, feature_columns)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (model_id) DO UPDATE SET
        performance_metric = EXCLUDED.performance_metric,
        created_at = CURRENT_TIMESTAMP;
        """
        
        postgres_hook.run(insert_classification_sql, parameters=(
            classification_info['model_id'],
            'classification',
            classification_info['model_name'],
            classification_info['accuracy'],
            'accuracy',
            classification_info['model_path'],
            ','.join(classification_info['feature_columns'])
        ))
        
        logging.info("Метадані моделей збережено в базу даних")
        
        return {
            'regression_model_id': regression_info['model_id'],
            'regression_performance': regression_info['r2_score'],
            'classification_model_id': classification_info['model_id'],
            'classification_performance': classification_info['accuracy'],
            'total_models_trained': 2
        }
        
    except Exception as e:
        logging.error(f"Помилка при оцінці моделей: {str(e)}")
        raise

def make_predictions(**context):
    """
    Створення прогнозів з використанням навчених моделей
    """
    try:
        evaluation_info = context['task_instance'].xcom_pull(task_ids='evaluate_models')
        regression_info = context['task_instance'].xcom_pull(task_ids='train_regression_model')
        classification_info = context['task_instance'].xcom_pull(task_ids='train_classification_model')
        
        regression_model = joblib.load(regression_info['model_path'])
        regression_scaler = joblib.load(regression_info['scaler_path'])
        classification_model = joblib.load(classification_info['model_path'])
        classification_scaler = joblib.load(classification_info['scaler_path'])
        
        postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
        df = pd.read_sql_query("SELECT * FROM starbucks_ml_raw LIMIT 10", 
                              postgres_hook.get_sqlalchemy_engine())
        
        le_size = joblib.load('/tmp/size_encoder.joblib')
        df['size_encoded'] = le_size.transform(df['size'])
        
        regression_features = df[regression_info['feature_columns']]
        if regression_info['model_name'] == 'linear_regression':
            regression_features_scaled = regression_scaler.transform(regression_features)
            calories_pred = regression_model.predict(regression_features_scaled)
        else:
            calories_pred = regression_model.predict(regression_features)
        
        classification_features = df[classification_info['feature_columns']]
        if classification_info['model_name'] == 'logistic_regression':
            classification_features_scaled = classification_scaler.transform(classification_features)
            caffeine_category_pred = classification_model.predict(classification_features_scaled)
        else:
            caffeine_category_pred = classification_model.predict(classification_features)
        
        predictions_df = df[['product_name', 'size', 'calories', 'caffeine_mg']].copy()
        predictions_df['predicted_calories'] = calories_pred
        predictions_df['predicted_caffeine_category'] = caffeine_category_pred
        predictions_df['calories_diff'] = predictions_df['calories'] - predictions_df['predicted_calories']
        
        engine = postgres_hook.get_sqlalchemy_engine()
        predictions_df.to_sql('ml_predictions', engine, if_exists='replace', index=False)
        
        logging.info("=== ПРИКЛАДИ ПРОГНОЗІВ ===")
        for idx, row in predictions_df.head().iterrows():
            logging.info(f"Продукт: {row['product_name']}")
            logging.info(f"  Реальні калорії: {row['calories']}, Прогноз: {row['predicted_calories']:.1f}")
            logging.info(f"  Прогнозована категорія кофеїну: {row['predicted_caffeine_category']}")
            logging.info(f"  Різниця калорій: {row['calories_diff']:.1f}")
            logging.info("---")
        
        logging.info(f"Прогнози збережено в таблицю ml_predictions")
        
        return {
            'predictions_count': len(predictions_df),
            'regression_model_used': regression_info['model_id'],
            'classification_model_used': classification_info['model_id']
        }
        
    except Exception as e:
        logging.error(f"Помилка при створенні прогнозів: {str(e)}")
        raise

create_tables_task = PostgresOperator(
    task_id='create_tables',
    postgres_conn_id='postgres_default',
    sql="""
    CREATE TABLE IF NOT EXISTS starbucks_ml_raw (
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

prepare_features_task = PythonOperator(
    task_id='prepare_ml_features',
    python_callable=prepare_ml_features,
    dag=dag,
)

train_regression_task = PythonOperator(
    task_id='train_regression_model',
    python_callable=train_regression_model,
    dag=dag,
)

train_classification_task = PythonOperator(
    task_id='train_classification_model',
    python_callable=train_classification_model,
    dag=dag,
)

evaluate_models_task = PythonOperator(
    task_id='evaluate_models',
    python_callable=evaluate_models,
    dag=dag,
)

make_predictions_task = PythonOperator(
    task_id='make_predictions',
    python_callable=make_predictions,
    dag=dag,
)

create_tables_task >> extract_task >> load_task >> prepare_features_task
prepare_features_task >> [train_regression_task, train_classification_task]
[train_regression_task, train_classification_task] >> evaluate_models_task >> make_predictions_task
