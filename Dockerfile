FROM apache/spark:3.3.3

USER root

# 1. Cài đặt các công cụ hỗ trợ và thư viện hệ thống cho Chrome
RUN apt-get update && apt-get install -y \
    wget \
    gnupg \
    unzip \
    libnss3 \
    libgbm1 \
    libasound2 \
    fonts-liberation \
    libatk-bridge2.0-0 \
    libgtk-3-0 \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# 2. Cài đặt Google Chrome bản ổn định
RUN wget -q -O - https://dl-ssl.google.com/linux/linux_signing_key.pub | apt-key add - \
    && echo "deb [arch=amd64] http://dl.google.com/linux/chrome/deb/ stable main" >> /etc/apt/sources.list.d/google.list \
    && apt-get update \
    && apt-get install -y google-chrome-stable \
    && rm -rf /var/lib/apt/lists/*

# 3. Cài đặt các thư viện Python
RUN pip install --no-cache-dir \
    kafka-python \
    requests \
    beautifulsoup4 \
    selenium \
    pandas \
    numpy \
    pyspark \
    webdriver_manager \
    lxml

# Trở lại user spark để chạy ứng dụng
USER spark