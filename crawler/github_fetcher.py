import requests
from typing import Dict, List, Optional
import psycopg2


class GitHubContentFetcher:
    """Class để fetch và lưu releases, commits từ GitHub"""

    def __init__(self, headers: Dict, db_conn: psycopg2.extensions.connection):
        self.api_url = "https://api.github.com/graphql"
        self.headers = headers
        self.conn = db_conn
        self.cursor = self.conn.cursor()

    def fetch_and_store_releases(self, owner: str, repo: str, repo_id: int, first: int = 10) -> List[int]:
        """Lấy và lưu releases của repository"""
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
        variables = {"owner": owner, "repo": repo, "first": first}
        response = requests.post(
            self.api_url,
            json={"query": query, "variables": variables},
            headers=self.headers
        )
        release_ids = []

        if response.status_code == 200:
            data = response.json()
            releases = data.get("data", {}).get("repository", {}).get("releases", {}).get("nodes", [])

            for release in releases:
                release_content = f"{release['name']} - {release['tagName']} - {release['publishedAt']} - {release['description']}"
                self.cursor.execute(
                    """
                    INSERT INTO release (id, content, repoid)
                    VALUES (nextval('release_id_seq'), %s, %s)
                    ON CONFLICT DO NOTHING
                    RETURNING id
                    """,
                    (release_content, repo_id)
                )
                release_id = self.cursor.fetchone()[0] if self.cursor.rowcount > 0 else None
                if release_id:
                    release_ids.append(release_id)
                self.conn.commit()

        return release_ids

    def fetch_and_store_commits(self, owner: str, repo: str, release_id: int, first: int = 10) -> None:
        """Lấy và lưu commits của repository"""
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
        variables = {"owner": owner, "repo": repo, "first": first}
        response = requests.post(
            self.api_url,
            json={"query": query, "variables": variables},
            headers=self.headers
        )

        if response.status_code == 200:
            data = response.json()
            commits = data.get("data", {}).get("repository", {}).get("defaultBranchRef", {}).get("target", {}).get(
                "history", {}).get("nodes", [])

            for commit in commits:
                self.cursor.execute(
                    """
                    INSERT INTO "commit" (hash, message, releaseid)
                    VALUES (%s, %s, %s)
                    ON CONFLICT DO NOTHING
                    """,
                    (commit['oid'], commit['message'], release_id)
                )
            self.conn.commit()

    def close(self):
        """Đóng cursor khi hoàn tất"""
        self.cursor.close()