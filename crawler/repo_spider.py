import scrapy
import json
import psycopg2
from psycopg2.extras import execute_values
from concurrent.futures import ThreadPoolExecutor
from api.config import Config
from crawler.settings import Settings


class RepoSpider(scrapy.Spider):
    name = "repos"
    api_url = Settings.api_url
    custom_settings = {
        "USER_AGENT": "GitHubCrawler/1.0",
        "DOWNLOAD_DELAY": 2,  # Reduced delay for faster crawling
        "LOG_LEVEL": "DEBUG",
        "CONCURRENT_REQUESTS": 10,  # Increased to handle 100 parallel requests
        "CONCURRENT_REQUESTS_PER_DOMAIN": 10,  # Increased for GitHub API
        "DOWNLOADER_MIDDLEWARES": {
            'crawler.middleware.rate_limit_middleware.RateLimitMiddleware': 100,
        },
        "SPIDER_MIDDLEWARES": {
            'scrapy.spidermiddlewares.referer.RefererMiddleware': None,
        },
        "DOWNLOAD_TIMEOUT": 10,  # Timeout 30 giây
        "AUTOTHROTTLE_ENABLED": True,  # Helps manage load on GitHub API
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
        self.executor = ThreadPoolExecutor(max_workers=8)  # Thread pool for DB writes
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
        release_repo_map = {}  # Ánh xạ giữa repo index và release content
        commit_requests = []

        # Thu thập dữ liệu repository và release
        for idx, edge in enumerate(repos):
            repo = edge['node']
            user = repo['owner']['login']
            name = repo['name']
            star = repo['stargazerCount']
            repo_data.append((user, name, star))
            self.last_star = star
            self.total_crawled += 1

            releases = repo.get("releases", {}).get("nodes", [])
            if releases:
                release = releases[0]
                release_content = f"{release['name']} - {release['tagName']} - {release['publishedAt']} - {release['description']}"
                release_repo_map[idx] = release_content  # Gán release content với chỉ số repo
                tag_name = release['tagName']
                if tag_name:
                    commit_requests.append((user, name, tag_name, idx))  # Lưu idx để ánh xạ sau

        # Chèn repo và lấy repo_ids
        if repo_data:
            future = self.executor.submit(self.batch_insert_repos, repo_data)
            repo_ids = future.result()
            self.logger.info(f"Inserted {len(repo_ids)} repositories into DB")

            # Tạo release_data với repoid chính xác
            release_data = []
            if release_repo_map and repo_ids:
                for idx, content in release_repo_map.items():
                    if idx < len(repo_ids):  # Đảm bảo không vượt quá số repo_ids
                        release_data.append((content, repo_ids[idx]))

                # Chèn release với repoid đúng
                future = self.executor.submit(self.batch_insert_releases, release_data)
                release_ids = future.result()
                self.logger.info(f"Inserted {len(release_ids)} releases into DB")

                # Ánh xạ release_ids với commit requests
                release_id_map = {repo_ids[commit_idx]: release_id
                                  for commit_idx, release_id in zip(release_repo_map.keys(), release_ids)
                                  if release_id}
                for owner, repo_name, tag_name, idx in commit_requests:
                    repo_id = repo_ids[idx] if idx < len(repo_ids) else None
                    if repo_id in release_id_map:
                        for request in self.fetch_commits(owner, repo_name, tag_name, release_id_map[repo_id]):
                            yield request

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
        self.logger.debug(f"Parsing commits for {owner}/{repo}, release_id: {release_id}, status: {response.status}")

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
            future.result()  # Ensure completion, but offloaded to thread
            self.logger.info(f"Batch inserted {len(commit_data)} commits for release_id: {release_id}")

        page_info = history.get("pageInfo", {})
        if page_info.get("hasNextPage", False):
            self.logger.debug(f"Fetching next page of commits for {owner}/{repo}, release_id: {release_id}")
            yield from self.fetch_commits(owner, repo, tag_name, release_id, page_info["endCursor"])
        else:
            self.logger.info(f"Finished fetching all commits for {owner}/{repo}, release_id: {release_id}")

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