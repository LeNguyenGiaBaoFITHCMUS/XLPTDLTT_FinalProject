#!/bin/bash

# ============================================================
# SETUP PERMISSIONS CHO HDFS
# Chạy script này 1 lần sau khi khởi động Docker
# ============================================================

echo "=================================================="
echo "  SETUP PERMISSIONS CHO HDFS"
echo "=================================================="
echo ""

# Kiểm tra namenode đã running chưa
if ! docker ps | grep -q namenode; then
    echo "Lỗi: Container namenode chưa chạy!"
    echo "Vui lòng chạy: docker-compose up -d"
    exit 1
fi

echo "Tạo các thư mục cần thiết trong HDFS..."

# Tạo thư mục nếu chưa có
docker exec namenode hdfs dfs -mkdir -p /datalake 2>/dev/null
docker exec namenode hdfs dfs -mkdir -p /warehouse 2>/dev/null
docker exec namenode hdfs dfs -mkdir -p /powerbi 2>/dev/null
docker exec namenode hdfs dfs -mkdir -p /checkpoints 2>/dev/null

echo "V Đã tạo thư mục"
echo ""

echo "Cấp quyền cho các thư mục..."

docker exec namenode hdfs dfs -chmod 777 /datalake
docker exec namenode hdfs dfs -chmod 777 /warehouse
docker exec namenode hdfs dfs -chmod 777 /powerbi
docker exec namenode hdfs dfs -chmod 777 /checkpoints

echo "✓ Đã cấp quyền"
echo ""

echo "Kiểm tra quyền:"
echo ""
docker exec namenode hdfs dfs -ls -d /datalake /warehouse /powerbi /checkpoints

echo ""
echo "=================================================="
echo "HOÀN TẤT SETUP PERMISSIONS"
echo "=================================================="
echo ""
echo "Giải thích quyền:"
echo "  drwxrwxrwx = 777"
echo "    - Owner (root): read(r), write(w), execute(x)"
echo "    - Group (supergroup): read(r), write(w), execute(x)"
echo "    - Others (spark/powerbi): read(r), write(w), execute(x)"
echo ""
echo "Spark có thể GHI dữ liệu ✓"
echo "Power BI có thể ĐỌC dữ liệu ✓"
echo ""
