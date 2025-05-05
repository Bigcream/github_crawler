# Sử dụng image Python chính thức
FROM python:3.9-slim

# Đặt thư mục làm việc
WORKDIR /app

# Sao chép requirements.txt từ build context (thư mục gốc) và cài đặt thư viện
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Sao chép toàn bộ mã dự án từ build context (thư mục gốc)
COPY . .

# Đặt lệnh mặc định để chạy RepoSpider
CMD ["scrapy", "crawl", "repos", "-a", "limit=5000"]