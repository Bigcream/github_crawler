class Config:
    DB_HOST = "postgres"  # Sử dụng tên dịch vụ PostgreSQL
    DB_NAME = "github_crawler"
    DB_USER = "crawler"
    DB_PASSWORD = "crawler"
    DB_PORT = "5432"
    REDIS_HOST = "redis"  # Hoặc IP của Redis server
    REDIS_PORT = 6379
    REDIS_DB = 0
