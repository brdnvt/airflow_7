@echo off
chcp 65001 > nul
echo.
echo ===============================================
echo    🚀 ЗАПУСК AIRFLOW ПРОЄКТА 🚀
echo ===============================================
echo.

:: Перевірка чи є Docker
echo [1/4] Перевірка Docker...
docker --version >nul 2>&1
if %errorlevel% neq 0 (
    echo ❌ Docker не знайдений! Спочатку встанови Docker Desktop
    echo 📥 Завантажити можна тут: https://www.docker.com/products/docker-desktop/
    pause
    exit /b 1
)
echo ✅ Docker знайдений

:: Перевірка чи запущений Docker
echo [2/4] Перевірка чи запущений Docker...
docker info >nul 2>&1
if %errorlevel% neq 0 (
    echo ❌ Docker не запущений! Запусти Docker Desktop спочатку
    pause
    exit /b 1
)
echo ✅ Docker запущений

:: Зупинка старих контейнерів (якщо є)
echo [3/4] Зупинка старих контейнерів...
docker-compose down >nul 2>&1

:: Запуск проєкта
echo [4/4] Запуск Airflow...
echo ⏳ Це може зайняти кілька хвилин при першому запуску...
docker-compose up -d

if %errorlevel% neq 0 (
    echo ❌ Помилка при запуску!
    pause
    exit /b 1
)

echo.
echo ===============================================
echo    🎉 УСПІШНИЙ ЗАПУСК! 🎉
echo ===============================================
echo.
echo 🌐 Веб-інтерфейс Airflow: http://localhost:8080
echo 👤 Логін: admin
echo 🔑 Пароль: admin
echo.
echo 📊 База даних PostgreSQL: localhost:5432
echo.
echo ===============================================
echo    КОРИСНІ КОМАНДИ:
echo ===============================================
echo.
echo 🔍 Переглянути логи:
echo    docker-compose logs -f
echo.
echo 🛑 Зупинити проєкт:
echo    docker-compose down
echo.
echo 🔄 Перезапустити:
echo    docker-compose restart
echo.
echo 🧹 Повне очищення (видалити всі дані):
echo    docker-compose down -v
echo.
echo ===============================================

:: Відкрити браузер автоматично
echo 🌐 Відкриваю веб-інтерфейс...
timeout /t 10 /nobreak >nul
start http://localhost:8080

echo.
echo ✨ Готово! Можеш закрити це вікно або натисни будь-яку клавішу
pause >nul
