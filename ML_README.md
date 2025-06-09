# Starbucks ML Pipeline - Machine Learning DAGs

Цей проєкт містить розширені DAG'и Apache Airflow для аналізу даних Starbucks з використанням машинного навчання.

## Структура проєкту

### Нові DAG'и:

1. **starbucks_ml_pipeline** - Основний ML конвеєр
2. **starbucks_ml_batch_prediction** - Batch прогнозування

## DAG 1: starbucks_ml_pipeline

### Опис
Основний конвеєр машинного навчання, який виконує повний цикл від завантаження даних до навчання моделей.

### Завдання та XCom:

1. **extract_data**
   - Зчитує дані з CSV файлу
   - **XCom передача**: Шлях до тимчасового файлу

2. **load_to_database**
   - Завантажує дані в PostgreSQL
   - **XCom отримання**: Шлях до файлу від `extract_data`
   - **XCom передача**: Кількість завантажених рядків

3. **prepare_ml_features**
   - Підготовка даних для ML (очищення, кодування)
   - Створення категорій кофеїну для класифікації
   - **XCom передача**: 
     ```python
     {
         'data_path': '/tmp/starbucks_ml_features.csv',
         'encoder_path': '/tmp/size_encoder.joblib',
         'rows_count': int,
         'features_count': int
     }
     ```

4. **train_regression_model**
   - Навчання моделей регресії (Linear Regression, Random Forest)
   - Прогнозування калорій на основі харчових характеристик
   - **XCom отримання**: Інформація про підготовлені дані
   - **XCom передача**:
     ```python
     {
         'model_path': '/tmp/best_regression_model.joblib',
         'scaler_path': '/tmp/regression_scaler.joblib',
         'model_name': 'linear_regression' або 'random_forest',
         'r2_score': float,
         'model_results': dict,
         'feature_columns': list,
         'model_id': 'regression_modelname_timestamp'
     }
     ```

5. **train_classification_model**
   - Навчання моделей класифікації (Logistic Regression, Random Forest)
   - Класифікація категорій кофеїну
   - **XCom отримання**: Інформація про підготовлені дані
   - **XCom передача**:
     ```python
     {
         'model_path': '/tmp/best_classification_model.joblib',
         'scaler_path': '/tmp/classification_scaler.joblib',
         'model_name': 'logistic_regression' або 'random_forest',
         'accuracy': float,
         'model_results': dict,
         'feature_columns': list,
         'model_id': 'classification_modelname_timestamp'
     }
     ```

6. **evaluate_models**
   - Оцінка та порівняння моделей
   - Збереження метаданих моделей в БД
   - **XCom отримання**: Інформація від обох моделей навчання
   - **XCom передача**:
     ```python
     {
         'regression_model_id': str,
         'regression_performance': float,
         'classification_model_id': str,
         'classification_performance': float,
         'total_models_trained': 2
     }
     ```

7. **make_predictions**
   - Створення прогнозів на тестових даних
   - **XCom отримання**: Результати оцінки та інформація про моделі
   - **XCom передача**:
     ```python
     {
         'predictions_count': int,
         'regression_model_used': str,
         'classification_model_used': str
     }
     ```

### Моделі машинного навчання:

#### Регресія (прогнозування калорій):
- **Ознаки**: size_encoded, milk, whip, serv_size_m_l, total_fat_g, sugar_g, caffeine_mg
- **Цільова змінна**: calories
- **Моделі**: Linear Regression, Random Forest Regressor
- **Метрика**: R² Score

#### Класифікація (категорії кофеїну):
- **Ознаки**: size_encoded, milk, whip, serv_size_m_l, calories, total_fat_g, sugar_g
- **Цільова змінна**: caffeine_category (decaf, low, medium, high, very_high)
- **Моделі**: Logistic Regression, Random Forest Classifier
- **Метрика**: Accuracy

## DAG 2: starbucks_ml_batch_prediction

### Опис
Конвеєр для batch прогнозування з використанням збережених моделей.

### Завдання та XCom:

1. **get_latest_models**
   - Отримання найновіших моделей з БД
   - **XCom передача**:
     ```python
     {
         'regression': {
             'model_id': str,
             'model_name': str,
             'performance': float,
             'model_path': str,
             'features': list
         },
         'classification': {
             'model_id': str,
             'model_name': str,
             'performance': float,
             'model_path': str,
             'features': list
         }
     }
     ```

2. **prepare_new_data**
   - Підготовка нових даних для прогнозування
   - **XCom передача**:
     ```python
     {
         'data_path': str,
         'records_count': int
     }
     ```

3. **batch_predict**
   - Виконання прогнозів на нових даних
   - **XCom отримання**: Інформація про моделі та дані
   - **XCom передача**:
     ```python
     {
         'predictions_made': int,
         'regression_model_used': str,
         'classification_model_used': str,
         'prediction_timestamp': str
     }
     ```

4. **generate_prediction_report**
   - Генерація звіту по прогнозуванню
   - **XCom отримання**: Результати прогнозування
   - **XCom передача**:
     ```python
     {
         'report_generated': bool,
         'total_predictions': int
     }
     ```

## Структура бази даних

### Таблиці, що створюються:

1. **starbucks_ml_raw** - Сирі дані Starbucks
2. **ml_models_metadata** - Метадані навчених моделей
3. **ml_predictions** - Прогнози основного конвеєра
4. **batch_predictions** - Результати batch прогнозування
5. **prediction_metrics** - Метрики прогнозування

## Використання XCom

### Ключові принципи передачі даних:

1. **Ідентифікатори моделей** - передаються між усіма ML завданнями
2. **Шляхи до файлів** - для збережених моделей та даних
3. **Метрики якості** - R² для регресії, Accuracy для класифікації
4. **Конфігурація моделей** - назви ознак, параметри

### Приклад використання XCom в коді:
```python
# Передача даних
return {
    'model_id': f"regression_rf_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
    'model_path': '/tmp/model.joblib',
    'performance': 0.85
}

# Отримання даних
model_info = context['task_instance'].xcom_pull(task_ids='train_model')
model_id = model_info['model_id']
```

## Запуск

1. Переконайтеся, що всі залежності встановлені:
   ```bash
   pip install -r requirements.txt
   ```

2. Запустіть Airflow:
   ```bash
   ./start_airflow.ps1
   ```

3. В веб-інтерфейсі увімкніть DAG'и:
   - `starbucks_ml_pipeline`
   - `starbucks_ml_batch_prediction`

4. Спочатку запустіть основний ML конвеєр, потім batch прогнозування

## Розширення

Проєкт можна розширити додаванням:
- Інших алгоритмів ML
- Hyperparameter tuning
- Model drift detection
- A/B тестування моделей
- Real-time прогнозування
