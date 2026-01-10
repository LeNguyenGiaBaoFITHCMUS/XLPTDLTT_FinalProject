#!/bin/bash
set -e

ADMIN_PASSWORD="admin123"

echo "========================================="
echo "KHỞI TẠO AIRFLOW VỚI PASSWORD CỐ ĐỊNH"
echo "========================================="

# Fix docker.sock permission (ignore error nếu không có quyền)
chmod 666 /var/run/docker.sock 2>/dev/null || true

# Khởi tạo database
airflow db migrate

# Tạo admin user với password cố định (skip nếu đã tồn tại)
airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password "$ADMIN_PASSWORD" 2>/dev/null || echo "User admin đã tồn tại"

echo "========================================="
echo "Username: admin"
echo "Password: $ADMIN_PASSWORD"
echo "========================================="

# Chạy Airflow standalone
exec airflow standalone
