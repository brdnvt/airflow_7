# Швидкий старт з Airflow ETL

## Мінімальні кроки для запуску

1. **Переконайтесь, що Docker Desktop запущено**

2. **Відкрийте PowerShell**
   ```powershell
   cd c:\Users\bardi\Downloads\airflow_6
   ```

3. **Запустіть скрипт**
   ```powershell
   .\start_airflow_final.ps1
   ```

4. **Відкрийте веб-інтерфейс**
   - URL: http://localhost:8080
   - Логін: `admin`
   - Пароль: `admin`

5. **Запустіть DAG**
   - Увімкніть DAG `starbucks_elt_pipeline`
   - Натисніть кнопку "▶️ Trigger DAG"

## Що далі?

- [PROJECT_DOCUMENTATION.md](PROJECT_DOCUMENTATION.md) - Повна документація проєкту
- [TECHNICAL_DOCUMENTATION.md](TECHNICAL_DOCUMENTATION.md) - Технічні деталі
- [USER_GUIDE.md](USER_GUIDE.md) - Детальне керівництво користувача
