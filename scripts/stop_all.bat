@echo off
echo ====================================================
echo Dung toan bo He thong Video Streaming Analytics
echo ====================================================

docker-compose -f deployments/docker-compose.mongodb.yml -f deployments/docker-compose.kafka.yml -f deployments/docker-compose.services.yml down

echo ====================================================
echo Da tat he thong!
echo ====================================================
pause
