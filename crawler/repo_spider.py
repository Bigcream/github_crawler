import scrapy
import json
import psycopg2
from api.config import Config
from crawler.settings import Settings


class RepoSpider(scrapy.Spider):
    name = "repos"
    custom_settings = {
        "USER_AGENT": "GitHubCrawler/1.0",
        "DOWNLOAD_DELAY": 1,
        "LOG_LEVEL": "INFO"
    }

    def __init__(self, limit=5000, *args, **kwargs):
        super(RepoSpider, self).__init__(*args, **kwargs)
        self.limit = int(limit)
        self.per_page = 100
        self.pages = (self.limit // self.per_page) + 1
        self.headers = Settings.HEADERS
        self.conn = psycopg2.connect(
            host=Config.DB_HOST,
            database=Config.DB_NAME,
            user=Config.DB_USER,
            password=Config.DB_PASSWORD,
            port=Config.DB_PORT
        )
        self.cursor = self.conn.cursor()

    def start_requests(self):
        for page in range(1, self.pages + 1):
            url = f"https://api.github.com/search/repositories?q=stars:>=1&sort=stars&order=desc&per_page={self.per_page}&page={page}"
            yield scrapy.Request(
                url=url,
                headers=self.headers,
                callback=self.parse
            )

    def parse(self, response):
        if response.status != 200:
            self.logger.error(f"Failed to fetch repos: {response.status}, {response.text}")
            return

        data = json.loads(response.text)
        repos = data.get("items", [])

        for repo in repos:
            star = repo["stargazers_count"]
            full_name = repo["full_name"]
            user, name = full_name.split("/")
            # Lưu vào bảng repo
            self.cursor.execute(
                """
                INSERT INTO repo ("user", name, star)
                VALUES (%s, %s, %s)
                ON CONFLICT DO NOTHING
                RETURNING id
                """,
                (user, name, star)
            )
            self.conn.commit()

    def closed(self, reason):
        self.cursor.close()
        self.conn.close()
