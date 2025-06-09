Write-Host "Запуск Airflow..." -ForegroundColor Green
Write-Host "Перевірка Docker..." -ForegroundColor Yellow

try {
    docker --version
    Write-Host "Docker знайдений" -ForegroundColor Green
} catch {
    Write-Host "Docker не знайдений!" -ForegroundColor Red
    exit 1
}

if (!(Test-Path "docker-compose.yml")) {
    Write-Host "docker-compose.yml не знайдений!" -ForegroundColor Red
    exit 1
}

Write-Host "Запуск контейнерів..." -ForegroundColor Yellow
docker-compose up -d

Write-Host "Веб-інтерфейс: http://localhost:8080" -ForegroundColor Cyan
Write-Host "Логін: admin, Пароль: admin" -ForegroundColor Cyan
