# Sử dụng base image Python
FROM python:3.9-slim

# Thiết lập thư mục làm việc
WORKDIR /app

# Cài đặt các gói cần thiết
RUN apt-get update && apt-get install -y \
    cron \
    curl \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Cài đặt Docker CLI
RUN curl -fsSL https://get.docker.com -o get-docker.sh \
    && sh get-docker.sh \
    && rm get-docker.sh

# Cài đặt Docker Compose CLI
RUN curl -L "https://github.com/docker/compose/releases/download/v2.23.0/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose \
    && chmod +x /usr/local/bin/docker-compose

# Cài đặt Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Sao chép script Python
COPY scale_workers.py .

# Sao chép file crontab
COPY crontab /etc/cron.d/scale-workers
# Thêm dòng mới vào file crontab
RUN echo "" >> /etc/cron.d/scale-workers
# Cấp quyền cho file crontab
RUN chmod 0644 /etc/cron.d/scale-workers

# Áp dụng cron job
RUN crontab /etc/cron.d/scale-workers

# Tạo file log cho cron
RUN touch /var/log/cron.log

# Command để chạy cron và giữ container chạy
CMD cron && tail -f /var/log/cron.log