import os
from dotenv import load_dotenv

load_dotenv()  # Load biến môi trường từ .env

BOT_NAME = 'github-crawler'
SPIDER_MODULES = ['crawler.spider', 'crawler.repo_spider']
NEWSPIDER_MODULE = 'crawler.spider, crawler.repo_spider'
ROBOTSTXT_OBEY = False


class Settings:
    GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
    if not GITHUB_TOKEN:
        raise ValueError("GITHUB_TOKEN is not set in environment variables")
    HEADERS = {
        "Authorization": f"Bearer {GITHUB_TOKEN}",  # Sửa thành Bearer
        "User-Agent": "GitHubCrawler/1.0",
        "Content-Type": "application/json"  # Thêm Content-Type cho GraphQL
    }
    api_url = "https://api.github.com/graphql"

    DOWNLOAD_DELAY = 2