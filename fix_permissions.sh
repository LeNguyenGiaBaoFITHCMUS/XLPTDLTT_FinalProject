#!/bin/bash

echo "============================================================"
echo "  FIX PERMISSIONS FOR AIRFLOW & DOCKER"
echo "============================================================"

# 1. Fix docker.sock permission
echo "[1/2] Cấp quyền cho docker.sock..."
sudo chmod 666 /var/run/docker.sock
echo "   V Docker socket: OK"

# 2. Fix powerbi_data folder permission
echo "[2/2] Cấp quyền cho powerbi_data/..."
sudo chmod -R 777 powerbi_data/
echo "   V powerbi_data: OK"

echo "============================================================"
echo "  DONE! Airflow có thể chạy mượt mà rồi."
echo "============================================================"
echo ""
echo "LƯU Ý: Chạy lại script này SAU MỖI LẦN RESTART Docker Desktop:"
echo "  ./fix_permissions.sh"
echo ""
