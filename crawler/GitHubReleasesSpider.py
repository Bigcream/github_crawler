import json
import os
from datetime import datetime

import psycopg2  # Thư viện để kết nối PostgreSQL
import scrapy

from api.config import Config

# Thay bằng Personal Access Token của bạn
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
HEADERS = {
    "Authorization": f"Bearer {GITHUB_TOKEN}",
    "Content-Type": "application/json",
    "User-Agent": "github-releases-crawler"
}

# GraphQL Query
TOP_REPOS_QUERY = """
query ($first: Int!, $after: String) {
  search(query: "stars:>1", type: REPOSITORY, first: $first, after: $after) {
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


class GitHubReleasesSpider(scrapy.Spider):
    name = "github_releases"
    api_url = "https://api.github.com/graphql"
    custom_settings = {
        'FEEDS': {
            f'github_releases_{datetime.now().strftime("%Y%m%d")}.jsonl': {
                'format': 'jsonlines',
                'encoding': 'utf8',
                'overwrite': False,
            },
        },
        'DOWNLOAD_DELAY': 1,
        'CONCURRENT_REQUESTS': 5,
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Kết nối database - thay thông tin connection của bạn
        self.conn = psycopg2.connect(
            host=Config.DB_HOST,
            database=Config.DB_NAME,
            user=Config.DB_USER,
            password=Config.DB_PASSWORD,
            port=Config.DB_PORT
        )
        self.cursor = self.conn.cursor()

    def start_requests(self):
        variables = {"first": 100, "after": None}
        yield scrapy.Request(
            url=self.api_url,
            method="POST",
            headers=HEADERS,
            body=json.dumps({"query": TOP_REPOS_QUERY, "variables": variables}),
            callback=self.parse,
            meta={"variables": variables, "count": 0}
        )

    def parse(self, response):
        data = json.loads(response.text)
        if 'data' not in data or 'search' not in data['data']:
            self.logger.error("Invalid response from GitHub API")
            return

        search_data = data['data']['search']
        edges = search_data['edges']
        current_count = response.meta['count']

        for edge in edges:
            repo = edge['node']
            user = repo['owner']['login']
            name = repo['name']
            star = repo['stargazerCount']

            # Lưu vào database
            try:
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
                repo_id = self.cursor.fetchone()
                repo_id = repo_id[0] if repo_id else None
            except psycopg2.Error as e:
                self.logger.error(f"Database error: {e}")
                self.conn.rollback()
                continue

            # Xử lý thông tin releases
            repo_info = {
                'owner': user,
                'name': name,
                'stars': star,
                'repo_id': repo_id,  # Thêm ID từ database nếu cần
                'releases': []
            }

            # for release in repo['releases']['nodes']:
            #     release_info = {
            #         'name': release['name'],
            #         'tag': release['tagName'],
            #         'published_at': release['publishedAt'],
            #         'description': release['description']
            #     }
            #     repo_info['releases'].append(release_info)

            yield repo_info
            current_count += 1

            if current_count >= 5000:
                self.logger.info("Reached 5000 repositories, stopping...")
                return

        # Pagination
        page_info = search_data['pageInfo']
        if page_info['hasNextPage'] and current_count < 5000:
            variables = {
                "first": 100,
                "after": page_info['endCursor']
            }
            yield scrapy.Request(
                url=self.api_url,
                method="POST",
                headers=HEADERS,
                body=json.dumps({"query": TOP_REPOS_QUERY, "variables": variables}),
                callback=self.parse,
                meta={"variables": variables, "count": current_count}
            )

    def closed(self, reason):
        # Đóng kết nối database khi spider hoàn thành
        self.cursor.close()
        self.conn.close()
        self.logger.info("Database connection closed")