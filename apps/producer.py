import time
import json
import random
import csv
from kafka import KafkaProducer
import os

# 1. Cấu hình Kafka Producer
# Lưu ý: 'kafka:29092' là địa chỉ nội bộ trong Docker network
producer = KafkaProducer(
    bootstrap_servers=['kafka:29092'],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')  # Chuyển dữ liệu thành JSON
)

TOPIC_NAME = 'transactions'
# Đường dẫn file bên trong container (do volume mapping ./apps -> /opt/spark/apps)
CSV_FILE_PATH = '/opt/spark/apps/User0_credit_card_transactions.csv'

def run_producer():
    print(f"--- Bắt đầu gửi dữ liệu từ {CSV_FILE_PATH} vào topic '{TOPIC_NAME}' ---")
    
    # Kiểm tra file có tồn tại không
    if not os.path.exists(CSV_FILE_PATH):
        print(f"LỖI: Không tìm thấy file tại {CSV_FILE_PATH}")
        return

    try:
        with open(CSV_FILE_PATH, mode='r', encoding='utf-8-sig') as csv_file:
            # Sử dụng DictReader để tự động map header thành key trong JSON
            csv_reader = csv.DictReader(csv_file)
            
            count = 0
            for row in csv_reader:
                # Gửi dòng dữ liệu vào Kafka
                producer.send(TOPIC_NAME, value=row)
                
                count += 1
                print(f"[{count}] Đã gửi giao dịch: User {row.get('User', 'N/A')} - Amount: {row.get('Amount', 'N/A')}")
                
                # Yêu cầu đề bài: Random thời gian từ 1s đến 5s
                sleep_time = random.uniform(1, 5)
                time.sleep(sleep_time)
                
    except KeyboardInterrupt:
        print("\nĐã dừng Producer.")
    except Exception as e:
        print(f"Đã xảy ra lỗi: {e}")
    finally:
        producer.close()

if __name__ == '__main__':
    run_producer()