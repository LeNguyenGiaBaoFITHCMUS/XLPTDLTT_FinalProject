#!/bin/bash

# ============================================================
# HƯỚNG DẪN CHẠY PIPELINE THỦ CÔNG
# ============================================================

echo "=================================================="
echo "  PIPELINE XỬ LÝ GIAO DỊCH THẺ TÍN DỤNG"
echo "=================================================="
echo ""

# Màu sắc
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' 

check_permissions() {
    local needs_fix=false
    
    local perm=$(docker exec namenode hdfs dfs -stat "%a" /warehouse 2>/dev/null)
    if [ "$perm" != "777" ]; then
        needs_fix=true
    fi
    
    if [ "$needs_fix" = true ]; then
        echo -e "${YELLOW} Phát hiện quyền chưa đúng, đang fix...${NC}"
        docker exec namenode hdfs dfs -chmod 777 /datalake /warehouse /powerbi /checkpoints 2>/dev/null
        echo -e "${GREEN}✓ Đã fix permissions${NC}"
    fi
}

show_menu() {
    echo -e "${GREEN}Chọn bước muốn chạy:${NC}"
    echo ""
    echo "  1. Chạy Producer (Kafka) - Gửi dữ liệu"
    echo "  2. Chạy Spark Streaming - Xử lý real-time"
    echo "  3. Kiểm tra dữ liệu trong HDFS"
    echo "  4. Chạy Daily Aggregate - Phân tích"
    echo "  5. Export cho Power BI"
    echo "  6. Xem kết quả trong HDFS"
    echo "  7. Chạy tất cả (tự động)"
    echo "  0. Thoát"
    echo ""
    echo -e "${YELLOW}Lưu ý: Bước 1 và 2 phải chạy CÙNG LÚC ở 2 terminal khác nhau!${NC}"
    echo ""
}

run_producer() {
    echo -e "${GREEN}=== BƯỚC 1: CHẠY PRODUCER ===${NC}"
    echo "Gửi dữ liệu vào Kafka với delay 1-5s..."
    echo ""
    docker exec -it spark-master python3 /opt/spark/apps/producer.py
}

run_streaming() {
    echo -e "${GREEN}=== BƯỚC 2: CHẠY SPARK STREAMING ===${NC}"
    echo "Xử lý real-time và lưu vào HDFS..."
    echo ""
    docker exec -u root spark-master spark-submit \
      --master local[*] \
      --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.3 \
      --driver-memory 1g \
      --executor-memory 1g \
      /opt/spark/apps/spark_streaming.py
}

check_hdfs() {
    echo -e "${GREEN}=== BƯỚC 3: KIỂM TRA DỮ LIỆU TRONG HDFS ===${NC}"
    echo ""
    
    echo "Cấu trúc thư mục HDFS:"
    docker exec namenode hdfs dfs -ls /
    
    echo ""
    echo "Dữ liệu clean (từ Spark Streaming):"
    docker exec namenode hdfs dfs -ls /datalake/transactions_clean/ 2>/dev/null || echo "Chưa có dữ liệu"
    
    echo ""
    echo "Kết quả phân tích (từ Daily Aggregate):"
    docker exec namenode hdfs dfs -ls /warehouse/ 2>/dev/null || echo "Chưa có dữ liệu"
    
    echo ""
    echo "Export Power BI:"
    docker exec namenode hdfs dfs -ls /powerbi/ 2>/dev/null || echo "Chưa có dữ liệu"
}

run_aggregate() {
    echo -e "${GREEN}=== BƯỚC 4: CHẠY DAILY AGGREGATE ===${NC}"
    echo "Phân tích dữ liệu và tạo 8 loại báo cáo..."
    echo ""
    
    check_permissions
    
    if docker exec namenode hdfs dfs -test -d /datalake/transactions_clean 2>/dev/null; then
        docker exec spark-master spark-submit /opt/spark/apps/daily_aggregate.py
    else
        echo -e "${RED}Lỗi: Chưa có dữ liệu trong /datalake/transactions_clean${NC}"
        echo "Vui lòng chạy Producer và Spark Streaming trước!"
        return 1
    fi
}

run_export() {
    echo -e "${GREEN}=== BƯỚC 5: EXPORT CHO POWER BI ===${NC}"
    echo "Export 8 datasets vào /powerbi/..."
    echo ""
    
    check_permissions
    
    if docker exec namenode hdfs dfs -test -d /warehouse 2>/dev/null; then
        docker exec spark-master spark-submit /opt/spark/apps/export_powerbi.py
    else
        echo -e "${RED}Lỗi: Chưa có dữ liệu phân tích trong /warehouse${NC}"
        echo "Vui lòng chạy Daily Aggregate trước!"
        return 1
    fi
}

show_results() {
    echo -e "${GREEN}=== BƯỚC 6: XEM KẾT QUẢ ===${NC}"
    echo ""
    
    echo "Danh sách các phân tích trong /warehouse:"
    docker exec namenode hdfs dfs -ls /warehouse/ 2>/dev/null
    
    echo ""
    echo "Danh sách datasets Power BI trong /powerbi:"
    docker exec namenode hdfs dfs -ls /powerbi/ 2>/dev/null
    
    echo ""
    echo -e "${YELLOW}Để xem nội dung file CSV, dùng lệnh:${NC}"
    echo "  docker exec namenode hdfs dfs -cat /powerbi/daily_summary/*.csv | head -20"
}

run_all() {
    echo -e "${YELLOW}Chế độ tự động - Chạy tất cả các bước${NC}"
    echo ""
    
    echo -e "${RED}LƯU Ý: Producer và Streaming cần chạy ở 2 terminal riêng!${NC}"
    echo "Chế độ này chỉ demo workflow, không thể chạy tự động 100%"
    echo ""
    
    read -p "Bạn đã chạy Producer và Streaming rồi? (y/n): " answer
    if [ "$answer" = "y" ]; then
        echo "Chờ 10 giây để có dữ liệu..."
        sleep 10
        check_hdfs
        sleep 2
        run_aggregate
        sleep 2
        run_export
        sleep 2
        show_results
    else
        echo "Vui lòng chạy Producer và Streaming trước!"
    fi
}

# Main menu loop
while true; do
    show_menu
    read -p "Nhập lựa chọn (0-7): " choice
    echo ""
    
    case $choice in
        1) run_producer ;;
        2) run_streaming ;;
        3) check_hdfs ;;
        4) run_aggregate ;;
        5) run_export ;;
        6) show_results ;;
        7) run_all ;;
        0) echo "Tạm biệt!"; exit 0 ;;
        *) echo -e "${RED}Lựa chọn không hợp lệ!${NC}" ;;
    esac
    
    echo ""
    echo "=================================================="
    read -p "Nhấn Enter để tiếp tục..."
    clear
done
