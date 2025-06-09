"""
Скрипт для тестування ML компонентів Airflow DAG'ів
"""

import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor, RandomForestClassifier
from sklearn.metrics import mean_squared_error, r2_score, accuracy_score
from sklearn.preprocessing import StandardScaler, LabelEncoder
import joblib
import os
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_data_loading():
    """Тестування завантаження даних"""
    try:
        csv_path = 'starbucks.csv'
        df = pd.read_csv(csv_path)
        logger.info(f"✅ Дані успішно завантажено: {len(df)} рядків, {len(df.columns)} колонок")
        logger.info(f"Колонки: {list(df.columns)}")
        return df
    except Exception as e:
        logger.error(f"❌ Помилка завантаження даних: {e}")
        return None

def test_feature_preparation(df):
    """Тестування підготовки ознак"""
    try:
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
        
        logger.info(f"✅ Ознаки підготовлено: {len(df_clean)} рядків після очищення")
        logger.info(f"Розподіл категорій кофеїну: {df_clean['caffeine_category'].value_counts().to_dict()}")
        
        return df_clean, le_size
        
    except Exception as e:
        logger.error(f"❌ Помилка підготовки ознак: {e}")
        return None, None

def test_regression_model(df):
    """Тестування регресійної моделі"""
    try:
        feature_columns = ['size_encoded', 'milk', 'whip', 'serv_size_m_l', 
                          'total_fat_g', 'sugar_g', 'caffeine_mg']
        target_column = 'calories'
        
        X = df[feature_columns]
        y = df[target_column]
        
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, random_state=42
        )
        
        model = RandomForestRegressor(n_estimators=50, random_state=42)
        model.fit(X_train, y_train)
        
        y_pred = model.predict(X_test)
        r2 = r2_score(y_test, y_pred)
        mse = mean_squared_error(y_test, y_pred)
        
        logger.info(f"✅ Регресійна модель навчена:")
        logger.info(f"   R² Score: {r2:.3f}")
        logger.info(f"   MSE: {mse:.2f}")
        logger.info(f"   RMSE: {np.sqrt(mse):.2f}")
        
        return model, r2
        
    except Exception as e:
        logger.error(f"❌ Помилка навчання регресійної моделі: {e}")
        return None, None

def test_classification_model(df):
    """Тестування класифікаційної моделі"""
    try:
        feature_columns = ['size_encoded', 'milk', 'whip', 'serv_size_m_l', 
                          'calories', 'total_fat_g', 'sugar_g']
        target_column = 'caffeine_category'
        
        X = df[feature_columns]
        y = df[target_column]
        
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, random_state=42, stratify=y
        )
        
        model = RandomForestClassifier(n_estimators=50, random_state=42)
        model.fit(X_train, y_train)
        
        y_pred = model.predict(X_test)
        accuracy = accuracy_score(y_test, y_pred)
        
        logger.info(f"✅ Класифікаційна модель навчена:")
        logger.info(f"   Accuracy: {accuracy:.3f}")
        
        unique, counts = np.unique(y_pred, return_counts=True)
        pred_distribution = dict(zip(unique, counts))
        logger.info(f"   Розподіл прогнозів: {pred_distribution}")
        
        return model, accuracy
        
    except Exception as e:
        logger.error(f"❌ Помилка навчання класифікаційної моделі: {e}")
        return None, None

def test_model_saving_loading():
    """Тестування збереження та завантаження моделей"""
    try:
        X = np.random.rand(100, 3)
        y = np.random.rand(100)
        
        model = RandomForestRegressor(n_estimators=10, random_state=42)
        model.fit(X, y)
        
        test_model_path = 'test_model.joblib'
        joblib.dump(model, test_model_path)
        
        loaded_model = joblib.load(test_model_path)
        
        pred1 = model.predict(X[:5])
        pred2 = loaded_model.predict(X[:5])
        
        if np.allclose(pred1, pred2):
            logger.info("✅ Збереження та завантаження моделей працює коректно")
        else:
            logger.error("❌ Різні прогнози після завантаження моделі")
        
        os.remove(test_model_path)
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Помилка тестування збереження моделей: {e}")
        return False

def test_xcom_simulation():
    """Симуляція роботи XCom"""
    try:
        task1_output = {
            'model_id': 'test_regression_rf_20250609_120000',
            'model_path': '/tmp/test_model.joblib',
            'r2_score': 0.85,
            'feature_columns': ['size_encoded', 'milk', 'whip']
        }
        
        received_model_id = task1_output['model_id']
        received_score = task1_output['r2_score']
        
        logger.info("✅ XCom симуляція:")
        logger.info(f"   Передано model_id: {received_model_id}")
        logger.info(f"   Передано r2_score: {received_score}")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Помилка XCom симуляції: {e}")
        return False

def main():
    """Основна функція тестування"""
    logger.info("🚀 Початок тестування ML компонентів")
    
    df = test_data_loading()
    if df is None:
        logger.error("❌ Не вдалося завантажити дані. Припинення тестування.")
        return
    
    df_prepared, encoder = test_feature_preparation(df)
    if df_prepared is None:
        logger.error("❌ Не вдалося підготувати ознаки. Припинення тестування.")
        return
    
    reg_model, reg_score = test_regression_model(df_prepared)
    
    clf_model, clf_score = test_classification_model(df_prepared)
    
    model_io_test = test_model_saving_loading()
    
    xcom_test = test_xcom_simulation()
    
    logger.info("\n" + "="*50)
    logger.info("📊 ПІДСУМОК ТЕСТУВАННЯ:")
    logger.info(f"   Завантаження даних: {'✅' if df is not None else '❌'}")
    logger.info(f"   Підготовка ознак: {'✅' if df_prepared is not None else '❌'}")
    logger.info(f"   Регресія (R²={reg_score:.3f}): {'✅' if reg_model is not None else '❌'}")
    logger.info(f"   Класифікація (Acc={clf_score:.3f}): {'✅' if clf_model is not None else '❌'}")
    logger.info(f"   Збереження моделей: {'✅' if model_io_test else '❌'}")
    logger.info(f"   XCom симуляція: {'✅' if xcom_test else '❌'}")
    
    success_count = sum([
        df is not None,
        df_prepared is not None,
        reg_model is not None,
        clf_model is not None,
        model_io_test,
        xcom_test
    ])
    
    logger.info(f"\n🎯 Результат: {success_count}/6 тестів пройдено успішно")
    
    if success_count == 6:
        logger.info("🎉 Всі ML компоненти готові до використання в Airflow!")
    else:
        logger.warning("⚠️ Деякі компоненти потребують налаштування")

if __name__ == "__main__":
    main()
