@echo off
echo 🚀 Starting Earth Observation Cube Environment...
echo ================================================

REM Check if Docker is running
docker info >nul 2>&1
if errorlevel 1 (
    echo ❌ Docker is not running. Please start Docker Desktop first.
    pause
    exit /b 1
)

REM Build and start the services
echo 📦 Building and starting services...
docker-compose up --build -d

REM Wait for services to be ready
echo ⏳ Waiting for services to be ready...
timeout /t 10 /nobreak >nul

REM Show service status
echo 📊 Service Status:
docker-compose ps

echo.
echo 🌐 Access URLs:
echo    Jupyter Lab: http://localhost:8888
echo    Dask Scheduler: http://localhost:8786
echo    Dask Dashboard: http://localhost:8787
echo.
echo 🔑 Jupyter Token: your_token_here
echo.
echo 📝 To stop the services, run: docker-compose down
echo 📝 To view logs, run: docker-compose logs -f
echo.
pause 