@echo off
echo ====================================================
echo KHOI DONG TOAN HE THONG Video Streaming Analytics
echo ====================================================

echo 1. Tao network "streaming_network" (neu chua co)
docker network create streaming_network 2>nul

echo 2. Khoi dong Ha tang (Kafka ^& MongoDB) va Cac Dich vu (Producer, Consumer, Dashboard)...
docker-compose -f deployments/docker-compose.mongodb.yml -f deployments/docker-compose.kafka.yml -f deployments/docker-compose.services.yml up -d --build

echo.
echo ====================================================
echo Hoan tat! He thong dang chay ngam.
echo Kafka UI: http://localhost:8080
echo Dashboard: http://localhost:8501
echo ====================================================
pause
