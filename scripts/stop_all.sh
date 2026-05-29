#!/bin/bash
echo "===================================================="
echo "DUNG TOAN BO HE THONG Video Streaming Analytics"
echo "===================================================="

docker-compose -f deployments/docker-compose.mongodb.yml -f deployments/docker-compose.kafka.yml -f deployments/docker-compose.services.yml down

echo "===================================================="
echo "DA TAT HE THONG!"
echo "===================================================="
