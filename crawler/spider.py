import scrapy
import json
import psycopg2
from api.config import Config
from crawler.settings import Settings


class ReleasesSpider(scrapy.Spider):
    name = "releases"

    def __init__(self, *args, **kwargs):
        super(ReleasesSpider, self).__init__(*args, **kwargs)
        self.headers = Settings.HEADERS
        self.conn = psycopg2.connect(
            host=Config.DB_HOST,
            database=Config.DB_NAME,
            user=Config.DB_USER,
            password=Config.DB_PASSWORD,
            port=Config.DB_PORT
        )
        self.cursor = self.conn.cursor()
        # Lấy danh sách repo từ DB
        self.cursor.execute("SELECT id, \"user\", name FROM repo")
        self.repos = [{"id": row[0], "full_name": f"{row[1]}/{row[2]}"} for row in self.cursor.fetchall()]

    def start_requests(self):
        for repo in self.repos:
            repo_name = repo["full_name"]
            repo_id = repo["id"]
            releases_url = f"https://api.github.com/repos/{repo_name}/releases"
            yield scrapy.Request(
                url=releases_url,
                headers=self.headers,
                callback=self.parse_releases,
                meta={"repo_name": repo_name, "repo_id": repo_id}
            )

    def parse_releases(self, response):
        # Giữ nguyên logic cũ
        repo_name = response.meta["repo_name"]
        repo_id = response.meta["repo_id"]
        releases = json.loads(response.text)

        for release in releases:
            release_id = release["id"]
            content = json.dumps({
                "name": release["name"],
                "tag_name": release["tag_name"],
                "published_at": release["published_at"],
                "description": release["body"]
            })
            self.cursor.execute(
                "INSERT INTO \"release\" (id, content, repoID) VALUES (%s, %s, %s) ON CONFLICT DO NOTHING",
                (release_id, content, repo_id)
            )
            self.conn.commit()

            if "commit_url" in release["target_commitish"]:
                commit_url = release.get("commit_url",
                                         f"https://api.github.com/repos/{repo_name}/commits/{release['target_commitish']}")
                yield scrapy.Request(
                    url=commit_url,
                    headers=self.headers,
                    callback=self.parse_commit,
                    meta={"release_id": release_id}
                )

    def parse_commit(self, response):
        # Giữ nguyên logic cũ
        commit_data = json.loads(response.text)
        hash_ = commit_data["sha"]
        message = commit_data["commit"]["message"]
        release_id = response.meta["release_id"]

        self.cursor.execute(
            "INSERT INTO \"commit\" (hash, message, releaseID) VALUES (%s, %s, %s) ON CONFLICT DO NOTHING",
            (hash_, message, release_id)
        )
        self.conn.commit()

    def closed(self, reason):
        self.cursor.close()
        self.conn.close()
