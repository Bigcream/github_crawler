# GitHub Crawler

Dự án thu thập thông tin release và commit từ 5000 repository có số sao cao nhất trên GitHub, sử dụng Scrapy và Celery, chạy trong Docker.

## Cài đặt

1. Cài đặt Docker và Docker Compose:
   ```
   sudo apt-get install docker.io docker-compose
   ```

2. Tạo file .env với nội dung sau:
      ```
   # .env
   GITHUB_TOKEN=your_github_token
      ```

## Chạy dự án

1. Xây dựng và chạy các dịch vụ:
   ```
   docker-compose up -d
   ```

2. Chạy Celery worker trong container `app`:
   ```
   docker-compose exec app celery -A github_crawler.tasks worker --loglevel=info --concurrency=4
   ```

3. Chạy Scrapy spider trong container `app` (mở terminal mới):
   ```
   docker-compose exec app bash
   cd github_crawler
   scrapy crawl repos
   ```

## Dừng dịch vụ
```
docker-compose down
```

## Ghi chú
- Đảm bảo PostgreSQL có schema `repo` (`user`, `name`, `star`, `id`), `release` (`content`, `repoid`, `id`), và `commit` (`hash`, `message`, `releaseid`).
- Điều chỉnh `concurrency` trong lệnh Celery tùy thuộc vào tài nguyên.
- Dữ liệu PostgreSQL và Redis được lưu trong volume (`postgres_data`, `redis_data`).