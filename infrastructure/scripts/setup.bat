@echo off
REM GTM Platform Setup Script
REM Runs the full platform setup from scratch

echo ================================================
echo   GTM Data Platform - Setup Script
echo ================================================

echo.
echo [1/6] Activating virtual environment...
call .venv\Scripts\activate

echo.
echo [2/6] Starting Docker services...
docker compose up -d
timeout /t 15

echo.
echo [3/6] Creating Kafka topics...
docker exec gtm-data-platform-kafka-1 kafka-topics --create --if-not-exists --topic apollo_leads --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
docker exec gtm-data-platform-kafka-1 kafka-topics --create --if-not-exists --topic crm_events --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
docker exec gtm-data-platform-kafka-1 kafka-topics --create --if-not-exists --topic product_usage --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
docker exec gtm-data-platform-kafka-1 kafka-topics --create --if-not-exists --topic lead_scores --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1

echo.
echo [4/6] Injecting test data...
python test_data_injector.py

echo.
echo [5/6] Running pipeline...
python simple_pipeline.py

echo.
echo [6/6] Running health check...
python monitoring\pipeline_health.py

echo.
echo ================================================
echo   Setup Complete!
echo ================================================
echo   Kafka UI  : http://localhost:8080
echo   Grafana   : http://localhost:3001
echo   Prometheus: http://localhost:9090
echo   n8n       : http://localhost:5678
echo   Metrics   : http://localhost:8000/metrics
echo ================================================