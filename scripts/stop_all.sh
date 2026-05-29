#!/bin/bash
echo "===================================================="
echo "Dừng toàn bộ Hệ thống Video Streaming Analytics"
echo "===================================================="

docker-compose -f deployments/docker-compose.mongodb.yml -f deployments/docker-compose.kafka.yml -f deployments/docker-compose.services.yml down

echo "===================================================="
echo "Đã tắt hệ thống!"
echo "===================================================="
