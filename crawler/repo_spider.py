import json

import psycopg2
import scrapy

from api.config import Config
from crawler.settings import Settings


class RepoSpider(scrapy.Spider):
    name = "repos"
    api_url = "https://api.github.com/graphql"
    custom_settings = {
        "USER_AGENT": "GitHubCrawler/1.0",
        "DOWNLOAD_DELAY": 5,
        "LOG_LEVEL": "DEBUG",
        "DOWNLOADER_MIDDLEWARES": {
            # 'crawler.RandomUserAgentMiddleware.RandomUserAgentMiddleware': 544,
            'crawler.RateLimitMiddleware.RateLimitMiddleware': 100,
            # 'scrapy.downloadermiddlewares.retry.RetryMiddleware': 90,
        },
        # "RETRY_ENABLED": True,
        # "RETRY_TIMES": 0,  # Tăng số lần retry lên 3
        # "RETRY_HTTP_CODES": [429, 502, 503, 504],  # Retry cho 502 và các mã lỗi khác
    }

    def __init__(self, limit=5000, *args, **kwargs):
        super(RepoSpider, self).__init__(*args, **kwargs)
        self.limit = int(limit)
        self.per_page = 100
        self.max_results_per_query = 1000
        self.headers = Settings.HEADERS
        self.conn = psycopg2.connect(
            host=Config.DB_HOST,
            database=Config.DB_NAME,
            user=Config.DB_USER,
            password=Config.DB_PASSWORD,
            port=Config.DB_PORT
        )
        self.cursor = self.conn.cursor()
        self.after_cursor = None
        self.last_star = None
        self.total_crawled = 0

    def start_requests(self):
        query = """
        query ($first: Int!, $after: String) {
          search(query: "stars:8000..414047 sort:stars-desc", type: REPOSITORY, first: $first, after: $after) {
            repositoryCount
            edges {
              node {
                ... on Repository {
                  name
                  owner {
                    login
                  }
                  stargazerCount
                  releases(first: 10, orderBy: {field: CREATED_AT, direction: DESC}) {
                    nodes {
                      name
                      tagName
                      publishedAt
                      description
                    }
                  }
                }
              }
              cursor
            }
            pageInfo {
              endCursor
              hasNextPage
            }
          }
        }
        """
        variables = {"first": self.per_page, "after": None}
        self.logger.debug(f"Starting request with query: {query}, variables: {variables}")
        yield scrapy.Request(
            url=self.api_url,
            method='POST',
            headers=self.headers,
            body=json.dumps({"query": query, "variables": variables}),
            callback=self.parse,
            meta={'page': 1, 'original_query': True},
            errback=self.handle_error  # Thêm xử lý lỗi
        )

    def parse(self, response):
        page = response.meta.get('page')
        original_query = response.meta.get('original_query', False)

        # Debug response
        self.logger.debug(f"Response status: {response.status}, headers: {response.headers}")

        if response.status != 200:
            self.logger.error(f"Failed to fetch repos: {response.status}, body: {response.text}")
            if response.status == 502:
                self.logger.warning("502 Bad Gateway received. Relying on retry middleware.")
            return

        data = json.loads(response.text)
        if 'errors' in data:
            self.logger.error(f"GraphQL errors: {data['errors']}")
            return

        search_data = data.get("data", {}).get("search", {})
        repos = search_data.get("edges", [])

        if not repos:
            self.logger.info("No more repositories to fetch.")
            return

        for edge in repos:
            repo = edge['node']
            user = repo['owner']['login']
            name = repo['name']
            star = repo['stargazerCount']

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
            self.last_star = star

        self.total_crawled += len(repos)
        self.logger.debug(f"Total crawled: {self.total_crawled}")

        if self.total_crawled >= self.limit:
            self.logger.info(f"Reached limit of {self.limit} repositories.")
            return

        page_info = search_data.get("pageInfo", {})
        if self.total_crawled < self.limit:
            self.after_cursor = page_info["endCursor"]
            query = """
            query ($first: Int!, $after: String) {
              search(query: "stars:8000..%s sort:stars-desc", type: REPOSITORY, first: $first, after: $after) {
                repositoryCount
                edges {
                  node {
                    ... on Repository {
                      name
                      owner {
                        login
                      }
                      stargazerCount
                      releases(first: 10, orderBy: {field: CREATED_AT, direction: DESC}) {
                        nodes {
                          name
                          tagName
                          publishedAt
                          description
                        }
                      }
                    }
                  }
                  cursor
                }
                pageInfo {
                  endCursor
                  hasNextPage
                }
              }
            }
            """ % self.last_star
            variables = {"first": self.per_page, "after": None}
            yield scrapy.Request(
                url=self.api_url,
                method='POST',
                headers=self.headers,
                body=json.dumps({"query": query, "variables": variables}),
                callback=self.parse,
                meta={'page': page + 1, 'original_query': original_query},
                errback=self.handle_error
            )

    def handle_error(self, failure):
        """Xử lý lỗi khi request thất bại"""
        self.logger.error(f"Request failed: {failure}")
        if failure.check(scrapy.exceptions.IgnoreRequest):
            self.logger.info("Request ignored due to middleware.")
        else:
            self.logger.warning("Unexpected error. Will retry via middleware.")

    def closed(self, reason):
        self.cursor.close()
        self.conn.close()