import scrapy
import json
import psycopg2
from psycopg2.extras import execute_values
from github_crawler.tasks import crawl_releases
from github_crawler.settings import Settings
from github_crawler.config import Config


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
            'github_crawler.middlewares.rate_limit_middleware.RateLimitMiddleware': 100,
        },
        "SPIDER_MIDDLEWARES": {
            'scrapy.spidermiddlewares.referer.RefererMiddleware': None,
        },
        "DOWNLOAD_TIMEOUT": 10,
        "AUTOTHROTTLE_ENABLED": True,
        "AUTOTHROTTLE_TARGET_CONCURRENCY": 10.0,
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
            headers=self.headers,
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
        for edge in repos:
            repo = edge['node']
            user = repo['owner']['login']
            name = repo['name']
            star = repo['stargazerCount']
            repo_data.append((user, name, star))
            self.last_star = star
            self.total_crawled += 1

        # Chèn repo và lấy repo_ids
        if repo_data:
            repo_ids = self.batch_insert_repos(repo_data)
            self.logger.info(f"Inserted {len(repo_ids)} repositories into DB")

            # Gửi tác vụ crawl releases cho Celery
            for idx, edge in enumerate(repos):
                repo = edge['node']
                user = repo['owner']['login']
                name = repo['name']
                repo_id = repo_ids[idx] if idx < len(repo_ids) else None
                if repo_id:
                    crawl_releases.delay(user, name, repo_id)

        self.logger.debug(f"Total crawled: {self.total_crawled}")
        if self.total_crawled >= self.limit:
            self.logger.info(f"Reached limit of {self.limit} repositories.")
            return

        # Phân trang
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
                headers=self.headers,
                body=json.dumps({"query": query, "variables": variables}),
                callback=self.parse,
                meta={'page': page + 1, 'original_query': original_query},
                errback=self.handle_error
            )

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
        self.logger.error(f"Request failed: {repr(failure)}")
        if failure.check(scrapy.exceptions.IgnoreRequest):
            self.logger.info("Request ignored due to middlewares.")
        else:
            self.logger.warning("Unexpected error. Will retry via middlewares.")

    def closed(self, reason):
        self.conn.close()
        self.logger.info(f"Spider closed with reason: {reason}")