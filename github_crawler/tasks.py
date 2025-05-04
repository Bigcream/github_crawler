import logging

from celery import Celery
import requests
import json
import psycopg2
from psycopg2.extras import execute_values
import time
from github_crawler.settings import Settings
from github_crawler.config import Config

# Cấu hình Celery với Redis làm message broker
app = Celery('github_crawler', broker='redis://localhost:6379/0')
app.conf.update(
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    timezone='UTC',
    enable_utc=True,
    task_annotations={
        'github_crawler.tasks.crawl_releases': {'rate_limit': '100/m'},
        'github_crawler.tasks.crawl_commits': {'rate_limit': '100/m'}
    }
)


# Tác vụ Celery để crawl releases
@app.task(bind=True, max_retries=0)
def crawl_releases(self, owner, repo_name, repo_id):
    try:
        query = """
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
        variables = {"owner": owner, "repo": repo_name}
        payload = json.dumps({"query": query, "variables": variables})
        response = requests.post(
            Settings.api_url,
            headers=Settings.HEADERS,
            data=payload,
            timeout=10
        )

        if response.status_code == 429:
            time.sleep(2 ** self.request.retries)
            raise self.retry(countdown=2 ** self.request.retries)
        if response.status_code != 200:
            print(f"Failed to fetch releases for {owner}/{repo_name}: {response.status_code}")
            return

        data = json.loads(response.text)
        releases = data.get("data", {}).get("repository", {}).get("releases", {}).get("nodes", [])
        if releases:
            release = releases[0]
            release_content = f"{release['name']} - {release['tagName']} - {release['publishedAt']} - {release['description']}"
            release_data = [(release_content, repo_id)]
            release_ids = batch_insert_releases(release_data)
            if release_ids and release['tagName']:
                crawl_commits.delay(owner, repo_name, release['tagName'], release_ids[0])

        return f"Completed crawling releases for {owner}/{repo_name}"
    except Exception as exc:
        raise self.retry(exc=exc, countdown=2 ** self.request.retries)


# Tác vụ Celery để crawl commits
@app.task(bind=True, max_retries=0)
def crawl_commits(self, owner, repo_name, tag_name, release_id, after = None):
    try:
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
        variables = {"owner": owner, "repo": repo_name, "tag": tag_name, "first": 100, "after": after}
        payload = json.dumps({"query": query, "variables": variables})
        response = requests.post(
            Settings.api_url,
            headers=Settings.HEADERS,
            data=payload,
            timeout=10
        )

        if response.status_code == 429:
            time.sleep(2 ** self.request.retries)
            raise self.retry(countdown=2 ** self.request.retries)
        if response.status_code != 200:
            print(f"Failed to fetch commits for {owner}/{repo_name}: {response.status_code}")
            return

        data = json.loads(response.text)
        history = data.get("data", {}).get("repository", {}).get("ref", {}).get("target", {}).get("history", {})
        commits = history.get("nodes", [])
        if commits:
            commit_data = [(commit['oid'], commit['message'], release_id) for commit in commits]
            batch_insert_commits(commit_data)

        page_info = history.get("pageInfo", {})
        if page_info.get("hasNextPage", False):
            print(f"Fetching next page of commits for {owner}/{repo_name}, cursor: {page_info['endCursor']}")
            # Gửi tác vụ mới với cursor tiếp theo
            crawl_commits.delay(owner, repo_name, tag_name, release_id, page_info["endCursor"])

        return f"Completed crawling commits for {owner}/{repo_name}, tag: {tag_name}"
    except Exception as exc:
        raise self.retry(exc=exc, countdown=2 ** self.request.retries)


def batch_insert_commits(commits):
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
                print(f"Error during batch commit insert: {e}")
                conn.rollback()


def batch_insert_releases(releases):
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
                return [row[0] for row in result] if result else []
            except Exception as e:
                print(f"Error during batch release insert: {e}")
                conn.rollback()
                return []