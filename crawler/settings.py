import itertools
import os
import logging

# Thiết lập logging
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

BOT_NAME = 'github-crawler'
SPIDER_MODULES = ['crawler.spider', 'crawler.repo_spider', 'crawler.release_commit_spider']
NEWSPIDER_MODULE = 'crawler.spider, crawler.repo_spider, crawler.release_commit_spider'
ROBOTSTXT_OBEY = False


class Settings:
    api_url = "https://api.github.com/graphql"
    DOWNLOAD_DELAY = 2

    def __init__(self):
        # Lấy GITHUB_TOKEN từ biến môi trường và split thành danh sách
        self.GITHUB_TOKENS = os.getenv("GITHUB_TOKEN").split(",") if os.getenv("GITHUB_TOKEN") else []

        # Kiểm tra nếu không có token
        if not self.GITHUB_TOKENS:
            raise ValueError("Không tìm thấy GITHUB_TOKEN trong biến môi trường hoặc danh sách token rỗng")

        # Tạo iterator để xoay vòng token
        self._token_cycle = itertools.cycle(self.GITHUB_TOKENS)

    def get_next_token(self):
        """Lấy token tiếp theo từ danh sách xoay vòng."""
        return next(self._token_cycle)

    def get_headers(self):
        """Tạo headers với token hiện tại."""
        token = self.get_next_token()
        logger.debug(f"Create headers with token: {token}")
        return {
            "Authorization": f"token {token.strip()}",
            "User-Agent": "GitHubCrawler/1.0"
        }
