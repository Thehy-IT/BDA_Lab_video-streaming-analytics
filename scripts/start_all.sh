#!/bin/bash
echo "===================================================="
echo "Khởi động toàn bộ Hệ thống Video Streaming Analytics"
echo "===================================================="

echo "1. Tạo network 'streaming_network' (nếu chưa có)"
docker network create streaming_network 2>/dev/null || true

echo "2. Khởi động Hạ tầng (Kafka & MongoDB) và Các Dịch vụ (Producer, Consumer, Dashboard)..."
docker-compose -f deployments/docker-compose.mongodb.yml -f deployments/docker-compose.kafka.yml -f deployments/docker-compose.services.yml up -d --build

echo ""
echo "===================================================="
echo "Hoàn tất! Hệ thống đang chạy ngầm."
echo "Kafka UI: http://localhost:8080"
echo "Dashboard: http://localhost:8501"
echo "===================================================="
