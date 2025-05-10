import scrapy
import json
import psycopg2
from psycopg2.extras import execute_values
from concurrent.futures import ThreadPoolExecutor
import redis
from api.config import Config
from crawler.settings import Settings


class RepoSpider(scrapy.Spider):
    name = "repos"
    api_url = Settings.api_url
    custom_settings = {
        "USER_AGENT": "GitHubCrawler/1.0",
        "DOWNLOAD_DELAY": 2,
        "LOG_LEVEL": "DEBUG",
        "CONCURRENT_REQUESTS": 10,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 10,
        "DOWNLOADER_MIDDLEWARES": {
            'crawler.middleware.rate_limit_middleware.RateLimitMiddleware': 100,
        },
        "SPIDER_MIDDLEWARES": {
            'scrapy.spidermiddlewares.referer.RefererMiddleware': None,
        },
        "DOWNLOAD_TIMEOUT": 30,
        "AUTOTHROTTLE_ENABLED": True,
        "AUTOTHROTTLE_TARGET_CONCURRENCY": 10.0,
    }

    def __init__(self, limit=5000, *args, **kwargs):
        super(RepoSpider, self).__init__(*args, **kwargs)
        self.limit = int(limit)
        self.per_page = 100
        self.Settings = Settings()
        self.conn = psycopg2.connect(
            host=Config.DB_HOST,
            database=Config.DB_NAME,
            user=Config.DB_USER,
            password=Config.DB_PASSWORD,
            port=Config.DB_PORT
        )
        self.redis_conn = redis.Redis(
            host=Config.REDIS_HOST,
            port=Config.REDIS_PORT,
            db=Config.REDIS_DB
        )
        self.executor = ThreadPoolExecutor(max_workers=8)
        self.after_cursor = None
        self.last_star = None
        self.total_crawled = 0

    def start_requests(self):
        query = """
        query ($first: Int!, $after: String) {
          search(query: "stars:8000..999999999 sort:stars-desc", type: REPOSITORY, first: $first, after: $after) {
            repositoryCount
            edges {
              node {
                ... on Repository {
                  name
                  owner {
                    login
                  }
                  stargazerCount
                  releases(first: 1, orderBy: {field: CREATED_AT, direction: DESC}) {
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
            headers=self.Settings.get_headers(),
            body=json.dumps({"query": query, "variables": variables}),
            callback=self.parse,
            meta={'page': 1, 'original_query': True},
            errback=self.handle_error
        )

    def parse(self, response):
        page = response.meta.get('page')
        original_query = response.meta.get('original_query', False)

        self.logger.debug(f"Parsing response for page {page}, status: {response.status}")
        if response.status != 200:
            self.log_error_to_db("crawler_repo", original_query, response.text, response.status)
            self.logger.error(f"Failed to fetch repos: {response.status}, body: {response.text}")
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

        repo_data = []
        repo_queue_data = []

        # Thu thập dữ liệu repository và release
        for idx, edge in enumerate(repos):
            repo = edge['node']
            user = repo['owner']['login']
            name = repo['name']
            star = repo['stargazerCount']
            repo_data.append((user, name, star))
            repo_queue_data.append({"owner": user, "repo_name": name})
            self.last_star = star
            self.total_crawled += 1

        # Chèn repo và lấy repo_ids
        if repo_data:
            future = self.executor.submit(self.batch_insert_repos, repo_data)
            repo_ids = future.result()
            self.logger.info(f"Inserted {len(repo_ids)} repositories into DB")

            # Đẩy vào Redis queue
            if repo_ids:
                for repo_id, queue_data in zip(repo_ids, repo_queue_data):
                    queue_entry = {
                        "repo_id": repo_id,
                        "owner": queue_data["owner"],
                        "repo_name": queue_data["repo_name"]
                    }
                    self.redis_conn.lpush("repos:requests", json.dumps(queue_entry))
                self.logger.info(f"Pushed {len(repo_ids)} repositories to Redis queue 'repos:requests'")

        self.logger.debug(f"Total crawled: {self.total_crawled}")
        if self.total_crawled >= self.limit:
            self.logger.info(f"Reached limit of {self.limit} repositories.")
            return

        # Phần phân trang giữ nguyên
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
                      releases(first: 1, orderBy: {field: CREATED_AT, direction: DESC}) {
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
            self.logger.debug(f"Paginating with query: {query}, variables: {variables}")
            yield scrapy.Request(
                url=self.api_url,
                method='POST',
                headers=self.Settings.get_headers(),
                body=json.dumps({"query": query, "variables": variables}),
                callback=self.parse,
                meta={'page': page + 1, 'original_query': original_query},
                errback=self.handle_error
            )

    def log_error_to_db(self, error_type, url, message, status):
        """Insert error details into the log_error table."""
        with psycopg2.connect(
                host=Config.DB_HOST,
                database=Config.DB_NAME,
                user=Config.DB_USER,
                password=Config.DB_PASSWORD,
                port=Config.DB_PORT
        ) as conn:
            with conn.cursor() as cursor:
                query = """
                INSERT INTO log_error (type, url, message, status)
                VALUES (%s, %s, %s, %s)
                """
                try:
                    cursor.execute(query, (error_type, url, message, status))
                    conn.commit()
                    self.logger.debug(f"Logged error to DB: {error_type}, {url}, {message}, {status}")
                except Exception as e:
                    self.logger.error(f"Failed to log error to DB: {e}")
                    conn.rollback()

    def batch_insert_repos(self, repos):
        with psycopg2.connect(
                host=Config.DB_HOST,
                database=Config.DB_NAME,
                user=Config.DB_USER,
                password=Config.DB_PASSWORD,
                port=Config.DB_PORT
        ) as conn:
            with conn.cursor() as cursor:
                query = """
                INSERT INTO repo ("user", name, star)
                VALUES %s
                ON CONFLICT DO NOTHING
                RETURNING id
                """
                try:
                    result = execute_values(cursor, query, repos, fetch=True)
                    conn.commit()
                    self.logger.debug(f"Batch inserted {len(result)} repos into DB")
                    return [row[0] for row in result] if result else []
                except Exception as e:
                    self.logger.error(f"Error during batch repo insert: {e}")
                    conn.rollback()
                    return []

    def handle_error(self, failure):
        """Handle request failures and log to log_error table."""
        self.logger.error(f"Request failed: {repr(failure)}")
        error_message = repr(failure)
        error_type = "crawler_repo"
        url = failure.request.url if hasattr(failure, 'request') else "N/A"

        self.log_error_to_db(error_type, url, error_message)
        if failure.check(scrapy.exceptions.IgnoreRequest):
            self.logger.info("Request ignored due to middleware.")
        else:
            self.logger.warning("Unexpected error. Will retry via middleware.")

    def closed(self, reason):
        self.executor.shutdown(wait=True)
        self.conn.close()
        self.redis_conn.close()
        self.logger.info(f"Spider closed with reason: {reason}")