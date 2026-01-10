#!/usr/bin/env python3
import os
import subprocess
import re

HDFS_PATH = "/powerbi"
LOCAL_DIR = "/opt/airflow/powerbi_data"
TEMP_DIR = "/tmp/powerbi_download"

def run_cmd(cmd, show_error=False):
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    if show_error and result.returncode != 0:
        print(f"[ERROR] Command failed: {cmd}")
        print(f"[ERROR] {result.stderr}")
    return result

print("=" * 60)
print("  DOWNLOAD DATASETS TỪ HDFS -> LOCAL")
print("=" * 60)

os.makedirs(LOCAL_DIR, exist_ok=True)
print(f"[1/4] Output folder: {LOCAL_DIR}")

result = run_cmd(f"docker exec namenode hdfs dfs -ls {HDFS_PATH}/")
if result.returncode != 0:
    print(f"[ERROR] Không thể list HDFS: {result.stderr}")
    exit(1)

csv_files = []
for line in result.stdout.split('\n'):
    if '.csv' in line:
        parts = line.split()
        if len(parts) >= 8:
            filename = parts[-1].split('/')[-1]
            csv_files.append(filename)

print(f"   Tìm thấy {len(csv_files)} files CSV")

# Tạo temp folder và download từng file
print("[3/4] Download files...")
run_cmd(f"docker exec namenode rm -rf {TEMP_DIR}")
run_cmd(f"docker exec namenode mkdir -p {TEMP_DIR}")

for filename in csv_files:
    run_cmd(f"docker exec namenode hdfs dfs -get {HDFS_PATH}/{filename} {TEMP_DIR}/{filename}", show_error=True)


temp_local = "/tmp/powerbi_temp"
run_cmd(f"rm -rf {temp_local}")
run_cmd(f"mkdir -p {temp_local}")
result = run_cmd(f"docker cp namenode:{TEMP_DIR}/. {temp_local}/", show_error=True)

if result.returncode == 0:
    # Check xem files có trong temp không
    check = run_cmd(f"ls {temp_local}/ | wc -l")
    print(f"   Files in temp: {check.stdout.strip()}")
    
    # Copy files bằng Python (tránh permission issue)
    import shutil
    temp_files = os.listdir(temp_local)
    copied = 0
    for fname in temp_files:
        if fname.endswith('.csv'):
            src = os.path.join(temp_local, fname)
            dst = os.path.join(LOCAL_DIR, fname)
            try:
                shutil.copy2(src, dst)
                copied += 1
            except Exception as e:
                print(f"   [WARN] {fname}: {e}")
    
    print(f"   ✓ Copy thành công {copied} files")
    run_cmd(f"rm -rf {temp_local}")
else:
    print("[ERROR] Không thể copy files từ namenode")

# Cleanup
run_cmd(f"docker exec namenode rm -rf {TEMP_DIR}")

# Kiểm tra kết quả
files = [f for f in os.listdir(LOCAL_DIR) if f.endswith('.csv')]
print(f"Đã download {len(files)} files")
print(f"Location: {LOCAL_DIR} (mapped to ./powerbi_data on host)")
if len(files) > 0:
    for f in sorted(files)[:10]:  # Show first 10
        size = os.path.getsize(os.path.join(LOCAL_DIR, f))
        print(f"   - {f:45s} {size:>10,} bytes")
    if len(files) > 10:
        print(f"   ... và {len(files) - 10} files khác")
print("=" * 60)

