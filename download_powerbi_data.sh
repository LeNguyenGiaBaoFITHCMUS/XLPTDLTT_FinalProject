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
mkdir -p "$POWERBI_DIR"

echo "Đang download từ HDFS..."
echo ""

# Danh sách datasets
datasets=(
    "hourly_analysis"
    "city_analysis"
    "merchant_analysis"
    "user_analysis"
    "high_value_analysis"
    "weekday_vs_weekend"
    "error_analysis"
    "daily_summary"
    "fraud_by_hour"
    "fraud_by_city"
    "fraud_by_merchant"
)

# Download từng dataset
for dataset in "${datasets[@]}"; do
    echo "  → Downloading $dataset..."
    
    docker exec namenode hdfs dfs -get "/powerbi/$dataset" /tmp/ 2>/dev/null
    
    docker cp "namenode:/tmp/$dataset" "$POWERBI_DIR/" 2>/dev/null

    docker exec namenode rm -rf "/tmp/$dataset" 2>/dev/null
    
    if [ -d "$POWERBI_DIR/$dataset" ]; then
        echo "    V Đã lưu vào: $POWERBI_DIR/$dataset/"
        
        # Tìm file CSV và hiển thị kích thước
        csv_file=$(find "$POWERBI_DIR/$dataset" -name "*.csv" -type f | head -1)
        if [ -n "$csv_file" ]; then
            size=$(du -h "$csv_file" | cut -f1)
            echo "  File CSV: $(basename "$csv_file") ($size)"
        fi
    else
        echo "    X Lỗi download"
    fi
    echo ""
done

echo "=================================================="
echo "HOÀN TẤT DOWNLOAD"
echo "=================================================="
echo ""
echo "Dữ liệu đã được lưu vào: $POWERBI_DIR/"
echo ""
echo "Cấu trúc thư mục:"
tree -L 2 "$POWERBI_DIR" 2>/dev/null || ls -lR "$POWERBI_DIR"

echo ""
echo "HƯỚNG DẪN IMPORT VÀO POWER BI:"
echo "  1. Mở Power BI Desktop"
echo "  2. Get Data → Text/CSV"
echo "  3. Chọn file .csv trong thư mục $POWERBI_DIR/"
echo "  4. Load data và tạo visualizations"
echo ""
