import os


BOT_NAME = 'github-crawler'
SPIDER_MODULES = ['crawler.spider', 'crawler.repo_spider']
NEWSPIDER_MODULE = 'crawler.spider, crawler.repo_spider'
ROBOTSTXT_OBEY = False


class Settings:
    token = os.getenv("GITHUB_TOKEN")  # Thay bằng token thực tế
    HEADERS = {
        "Authorization": f"token {token}",
        "User-Agent": "GitHubCrawler/1.0"
    }