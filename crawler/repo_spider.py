import scrapy
import json
import psycopg2

from api.config import Config
from crawler.settings import Settings


class RepoSpider(scrapy.Spider):
    name = "repos"
    api_url = Settings.api_url
    custom_settings = {
        "USER_AGENT": "GitHubCrawler/1.0",
        "DOWNLOAD_DELAY": 5,
        "LOG_LEVEL": "DEBUG",
        "DOWNLOADER_MIDDLEWARES": {
            'crawler.middleware.rate_limit_middleware.RateLimitMiddleware': 100,
        },
        "REFERRER_POLICY": "strict-origin-when-cross-origin",  # Giá trị hợp lệ
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
            errback=self.handle_error
        )

    def parse(self, response):
        page = response.meta.get('page')
        original_query = response.meta.get('original_query', False)

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

            with self.conn.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO repo ("user", name, star)
                    VALUES (%s, %s, %s)
                    ON CONFLICT DO NOTHING
                    RETURNING id
                    """,
                    (user, name, star)
                )
                repo_id = cursor.fetchone()[0] if cursor.rowcount > 0 else None
                self.conn.commit()

            if repo_id:
                query = """
                    query ($owner: String!, $repo: String!, $first: Int!) {
                        repository(owner: $owner, name: $repo) {
                            releases(first: $first, orderBy: {field: CREATED_AT, direction: DESC}) {
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
                variables = {"owner": user, "repo": name, "first": 10}  # Tăng lên 10
                payload = json.dumps({"query": query, "variables": variables})
                self.logger.debug(f"Sending request to fetch releases for {user}/{name}")
                yield scrapy.Request(
                    url=self.api_url,
                    method='POST',
                    headers=self.headers,
                    body=payload,
                    callback=self.parse_releases,
                    meta={'repo_id': repo_id, 'owner': user, 'repo': name},
                    errback=self.handle_error
                )

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

    def parse_releases(self, response):
        repo_id = response.meta['repo_id']
        owner = response.meta['owner']
        repo = response.meta['repo']
        release_ids = []

        if response.status == 200:
            data = json.loads(response.text)
            releases = data.get("data", {}).get("repository", {}).get("releases", {}).get("nodes", [])
            self.logger.debug(f"Releases data for {owner}/{repo}: {releases}")

            for release in releases:
                release_content = f"{release['name']} - {release['tagName']} - {release['publishedAt']} - {release['description']}"
                self.cursor.execute(
                    """
                    INSERT INTO release (content, repoid)
                    VALUES (%s, %s)
                    ON CONFLICT DO NOTHING
                    RETURNING id
                    """,
                    (release_content, repo_id)
                )
                release_id = self.cursor.fetchone()[0] if self.cursor.rowcount > 0 else None
                self.logger.debug(f"Inserted release: {release_content}, release_id: {release_id}")
                if release_id:
                    release_ids.append(release_id)
                self.conn.commit()

        if release_ids:
            latest_release_id = release_ids[-1]
            query = """
                query ($owner: String!, $repo: String!, $first: Int!) {
                    repository(owner: $owner, name: $repo) {
                        defaultBranchRef {
                            target {
                                ... on Commit {
                                    history(first: $first) {
                                        nodes {
                                            oid
                                            message
                                            committedDate
                                            author {
                                                name
                                                email
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                """
            variables = {"owner": owner, "repo": repo, "first": 10}  # Tăng lên 10
            payload = json.dumps({"query": query, "variables": variables})
            self.logger.debug(f"Sending request to fetch commits for release_id: {latest_release_id}")
            yield scrapy.Request(
                url=self.api_url,
                method='POST',
                headers=self.headers,
                body=payload,
                callback=self.parse_commits,
                meta={'release_id': latest_release_id},
                errback=self.handle_error
            )

    def parse_commits(self, response):
        release_id = response.meta['release_id']

        if response.status == 200:
            data = json.loads(response.text)
            commits = data.get("data", {}).get("repository", {}).get("defaultBranchRef", {}).get("target", {}).get("history", {}).get("nodes", [])
            self.logger.debug(f"Commits data for release_id {release_id}: {commits}")

            for commit in commits:
                self.cursor.execute(
                    """
                    INSERT INTO "commit" (hash, message, releaseid)
                    VALUES (%s, %s, %s)
                    ON CONFLICT DO NOTHING
                    """,
                    (commit['oid'], commit['message'], release_id)
                )
                self.logger.debug(f"Inserted commit: {commit['oid']} for release_id: {release_id}")
            self.conn.commit()

    def handle_error(self, failure):
        self.logger.error(f"Request failed: {failure}")
        self.logger.error(f"Failure details: {repr(failure)}")
        if failure.check(scrapy.exceptions.IgnoreRequest):
            self.logger.info("Request ignored due to middleware.")
        else:
            self.logger.warning("Unexpected error. Will retry via middleware.")

    def closed(self, reason):
        self.cursor.close()
        self.conn.close()