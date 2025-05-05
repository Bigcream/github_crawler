# Sử dụng image Python chính thức
FROM python:3.9-slim

# Đặt thư mục làm việc
WORKDIR /app

# Sao chép requirements.txt và cài đặt thư viện
COPY ../../requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Sao chép toàn bộ mã dự án
COPY ../.. .

# Đặt lệnh mặc định để chạy Scrapy worker
CMD ["scrapy", "crawl", "repos", "-a", "limit=5000"]

#CMD ["scrapy", "crawl", "release_commits"]