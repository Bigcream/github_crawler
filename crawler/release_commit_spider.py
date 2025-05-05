import scrapy
import json
import psycopg2
from psycopg2.extras import execute_values
from concurrent.futures import ThreadPoolExecutor
from scrapy_redis.spiders import RedisSpider
from api.config import Config
from crawler.settings import Settings


class ReleaseCommitSpider(RedisSpider):
    name = "release_commits"
    api_url = Settings.api_url
    redis_key = "repos:requests"
    custom_settings = {
        "USER_AGENT": "GitHubCrawler/1.0",
        "DOWNLOAD_DELAY": 2,
        "LOG_LEVEL": "DEBUG",
        "CONCURRENT_REQUESTS": 10,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 10,
        # "DOWNLOADER_MIDDLEWARES": {
        #     'crawler.middleware.rate_limit_middleware.RateLimitMiddleware': 100,
        # },
        "SPIDER_MIDDLEWARES": {
            'scrapy.spidermiddlewares.referer.RefererMiddleware': None,
        },
        "DOWNLOAD_TIMEOUT": 30,
        "AUTOTHROTTLE_ENABLED": True,
        "AUTOTHROTTLE_TARGET_CONCURRENCY": 10.0,
        "REDIS_URL": f"redis://{Config.REDIS_HOST}:{Config.REDIS_PORT}/{Config.REDIS_DB}",
        "SCHEDULER": "scrapy_redis.scheduler.Scheduler",
        "DUPEFILTER_CLASS": "scrapy_redis.dupefilter.RFPDupeFilter",
        "SCHEDULER_PERSIST": True,
        "SCHEDULER_QUEUE_CLASS": "scrapy_redis.queue.SpiderQueue",
    }

    def __init__(self, *args, **kwargs):
        super(ReleaseCommitSpider, self).__init__(*args, **kwargs)
        self.headers = Settings.HEADERS
        self.conn = psycopg2.connect(
            host=Config.DB_HOST,
            database=Config.DB_NAME,
            user=Config.DB_USER,
            password=Config.DB_PASSWORD,
            port=Config.DB_PORT
        )
        self.executor = ThreadPoolExecutor(max_workers=8)

    def make_request_from_data(self, data):
        queue_entry = json.loads(data.decode('utf-8'))
        repo_id = queue_entry['repo_id']
        owner = queue_entry['owner']
        repo_name = queue_entry['repo_name']
        self.logger.debug(f"Processing queue entry: repo_id={repo_id}, owner={owner}, repo_name={repo_name}")
        return scrapy.Request(
            url=self.api_url,
            method='POST',
            headers=self.headers,
            body=json.dumps({"query": self.get_release_query(), "variables": {"owner": owner, "repo": repo_name}}),
            callback=self.parse_releases,
            meta={'repo_id': repo_id, 'owner': owner, 'repo_name': repo_name},
            errback=self.handle_error
        )

    def get_release_query(self):
        return """
        query ($owner: String!, $repo: String!) {
          repository(owner: $owner, name: $repo) {
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
        """

    def parse_releases(self, response):
        repo_id = response.meta['repo_id']
        owner = response.meta['owner']
        repo_name = response.meta['repo_name']
        self.logger.debug(f"Parsing releases for {owner}/{repo_name}, repo_id: {repo_id}, status: {response.status}")

        if response.status != 200:
            self.logger.error(f"Failed to fetch releases: {response.status}, body: {response.text}")
            return

        data = json.loads(response.text)
        if 'errors' in data:
            self.logger.error(f"GraphQL errors: {data['errors']}")
            return

        releases = data.get("data", {}).get("repository", {}).get("releases", {}).get("nodes", [])
        if not releases:
            self.logger.info(f"No releases found for {owner}/{repo_name}")
            return

        release = releases[0]
        release_content = f"{release['name']} - {release['tagName']} - {release['publishedAt']} - {release['description']}"
        future = self.executor.submit(self.batch_insert_releases, [(release_content, repo_id)])
        release_ids = future.result()
        if release_ids:
            release_id = release_ids[0]
            self.logger.info(f"Inserted release for repo_id: {repo_id}, release_id: {release_id}")
            tag_name = release['tagName']
            if tag_name:
                for request in self.fetch_commits(owner, repo_name, tag_name, release_id):
                    yield request

    def fetch_commits(self, owner, repo, tag_name, release_id, after=None):
        query = """
        query ($owner: String!, $repo: String!, $tag: String!, $first: Int!, $after: String) {
            repository(owner: $owner, name: $repo) {
                ref(qualifiedName: $tag) {
                    target {
                        ... on Commit {
                            history(first: $first, after: $after) {
                                nodes {
                                    oid
                                    message
                                    committedDate
                                    author {
                                        name
                                        email
                                    }
                                }
                                pageInfo {
                                    endCursor
                                    hasNextPage
                                }
                            }
                        }
                    }
                }
            }
        }
        """
        variables = {"owner": owner, "repo": repo, "tag": tag_name, "first": 100, "after": after}
        payload = json.dumps({"query": query, "variables": variables})
        self.logger.debug(f"Requesting commits for {owner}/{repo} at tag {tag_name}, after: {after or 'None'}")
        yield scrapy.Request(
            url=self.api_url,
            method='POST',
            headers=self.headers,
            body=payload,
            callback=self.parse_commits,
            meta={'release_id': release_id, 'owner': owner, 'repo': repo, 'tag_name': tag_name},
            errback=self.handle_error
        )

    def parse_commits(self, response):
        release_id = response.meta['release_id']
        owner = response.meta['owner']
        repo = response.meta['repo']
        tag_name = response.meta['tag_name']
        self.logger.debug(f"Parsing commits for {owner}/{repo}, issues_id: {release_id}, status: {response.status}")

        if response.status != 200:
            self.logger.error(f"Failed to fetch commits: {response.status}")
            return

        data = json.loads(response.text)
        ref_data = data.get("data", {}).get("repository", {}).get("ref", {})
        if not ref_data:
            self.logger.warning(f"No ref data found for {owner}/{repo} at tag {tag_name}")
            return

        history = ref_data.get("target", {}).get("history", {})
        commits = history.get("nodes", [])
        if commits:
            commit_data = [(commit['oid'], commit['message'], release_id) for commit in commits]
            future = self.executor.submit(self.batch_insert_commits, commit_data)
            future.result()
            self.logger.info(f"Batch inserted {len(commit_data)} commits for release_id: {release_id}")

        page_info = history.get("pageInfo", {})
        if page_info.get("hasNextPage", False):
            self.logger.debug(f"Fetching next page of commits for {owner}/{repo}, release_id: {release_id}")
            yield from self.fetch_commits(owner, repo, tag_name, release_id, page_info["endCursor"])
        else:
            self.logger.info(f"Finished fetching all commits for {owner}/{repo}, release_id: {release_id}")

    def batch_insert_releases(self, releases):
        with psycopg2.connect(
            host=Config.DB_HOST,
            database=Config.DB_NAME,
            user=Config.DB_USER,
            password=Config.DB_PASSWORD,
            port=Config.DB_PORT
        ) as conn:
            with conn.cursor() as cursor:
                query = """
                INSERT INTO release (content, repoid)
                VALUES %s
                ON CONFLICT DO NOTHING
                RETURNING id
                """
                try:
                    result = execute_values(cursor, query, releases, fetch=True)
                    conn.commit()
                    self.logger.debug(f"Batch inserted {len(result)} releases into DB")
                    return [row[0] for row in result] if result else []
                except Exception as e:
                    self.logger.error(f"Error during batch release insert: {e}")
                    conn.rollback()
                    return []

    def batch_insert_commits(self, commits):
        with psycopg2.connect(
            host=Config.DB_HOST,
            database=Config.DB_NAME,
            user=Config.DB_USER,
            password=Config.DB_PASSWORD,
            port=Config.DB_PORT
        ) as conn:
            with conn.cursor() as cursor:
                query = """
                INSERT INTO "commit" (hash, message, releaseid)
                VALUES %s
                ON CONFLICT DO NOTHING
                """
                try:
                    execute_values(cursor, query, commits)
                    conn.commit()
                except Exception as e:
                    self.logger.error(f"Error during batch commit insert: {e}")
                    conn.rollback()

    def handle_error(self, failure):
        self.logger.error(f"Request failed: {repr(failure)}")
        if failure.check(scrapy.exceptions.IgnoreRequest):
            self.logger.info("Request ignored due to middleware.")
        else:
            self.logger.warning("Unexpected error. Will retry via middleware.")

    def closed(self, reason):
        self.executor.shutdown(wait=True)
        self.conn.close()
        self.logger.info(f"Spider closed with reason: {reason}")