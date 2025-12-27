FROM apache/spark:3.3.3

USER root

# Cài đặt thư viện cần thiết cho đồ án
RUN pip install --no-cache-dir \
    kafka-python \
    requests \
    beautifulsoup4 \
    pandas \
    numpy

USER spark