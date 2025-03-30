import os


BOT_NAME = 'github-crawler'
SPIDER_MODULES = ['crawler.spider', 'crawler.repo_spider']
NEWSPIDER_MODULE = 'crawler.spider, crawler.repo_spider'
ROBOTSTXT_OBEY = False


class Settings:
    GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")  # Thay bằng token thực tế
    HEADERS = {
        "Authorization": f"token {GITHUB_TOKEN}",
        "User-Agent": "GitHubCrawler/1.0"
    }
    api_url = "https://api.github.com/graphql"

    # Tùy chọn tối ưu hóa
    # RETRY_TIMES = 5
    # RETRY_HTTP_CODES = [500, 503, 504, 400, 403, 404, 408]
    DOWNLOAD_DELAY = 2  # Tăng delay để tránh bị chặn