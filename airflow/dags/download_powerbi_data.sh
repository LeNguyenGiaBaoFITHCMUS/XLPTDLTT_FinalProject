#!/bin/bash

# ============================================================
# DOWNLOAD DỮ LIỆU TỪ HDFS CHO POWER BI
# ============================================================

echo "=================================================="
echo "  DOWNLOAD DATASETS CHO POWER BI"
echo "=================================================="
echo ""

# Tạo thư mục local
POWERBI_DIR="./powerbi_data"

# Tạo thư mục local nếu chưa có
if [ ! -d "$POWERBI_DIR" ]; then
    mkdir -p "$POWERBI_DIR"
    echo "[INFO] Đã tạo thư mục: $POWERBI_DIR"
else
    echo "[INFO] Đã có thư mục: $POWERBI_DIR, xóa dữ liệu cũ trong thư mục"
    rm -rf $POWERBI_DIR/*
fi

echo "Bắt đầu download từ HDFS..."
echo ""
echo "[1/3] Chuẩn bị dữ liệu trên NameNode..."
# Tạo thư mục tạm trên container NameNode để gom file
# Dùng bash -c để xử lý ký tự wildcard bên trong container
docker exec namenode bash -c "rm -rf /tmp/powerbi_export && mkdir -p /tmp/powerbi_export"

# Copy tất cả file .csv từ HDFS /powerbi vào thư mục tạm
docker exec namenode bash -c "hdfs dfs -get /powerbi/*.csv /tmp/powerbi_export/"

if [ $? -eq 0 ]; then
    echo "-> Đã lấy dữ liệu từ HDFS thành công."
else
    echo "[ERROR] Không tìm thấy file .csv nào trong /powerbi trên HDFS!"
    exit 1
fi

echo ""
echo "[2/3] Copy dữ liệu từ Docker ra máy thật..."
# Copy từ container ra máy thật
docker cp namenode:/tmp/powerbi_export/. "$POWERBI_DIR/"

if [ $? -eq 0 ]; then
    echo "-> Copy thành công."
else
    echo "[ERROR] Lỗi khi copy."
    exit 1
fi

echo ""
echo "[3/3] Dọn dẹp..."
# Xóa thư mục tạm trong container để giải phóng dung lượng
docker exec namenode rm -rf /tmp/powerbi_export
echo "-> Đã dọn dẹp file tạm."
echo ""
echo "=================================================="
echo "HOÀN TẤT DOWNLOAD"
echo "=================================================="
echo ""
echo "Dữ liệu đã được lưu vào: $POWERBI_DIR/"
echo ""
echo "Cấu trúc thư mục:"
tree -L 2 "$POWERBI_DIR" 2>/dev/null || ls -lR "$POWERBI_DIR"

echo ""
echo "HƯỚNG DẪN IMPORT VÀO POWER BI ở local:"
echo "  1. Mở Power BI Desktop"
echo "  2. Get Data → Text/CSV"
echo "  3. Chọn file .csv trong thư mục $POWERBI_DIR/"
echo "  4. Load data và tạo visualizations"
echo ""