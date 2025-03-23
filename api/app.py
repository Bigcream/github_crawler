import json

import psycopg2
import schedule
from flask import Flask, jsonify, request
from scrapy.crawler import CrawlerProcess
import time
from api.config import Config
from crawler.GitHubReleasesSpider import GitHubReleasesSpider
from crawler.repo_spider import RepoSpider
from crawler.spider import ReleasesSpider

app = Flask(__name__)

is_running = False
# Kết nối PostgreSQL
def get_db_connection():
    return psycopg2.connect(
        host=Config.DB_HOST,
        database=Config.DB_NAME,
        user=Config.DB_USER,
        password=Config.DB_PASSWORD,
        port=Config.DB_PORT
    )


@app.route("/top-repos", methods=["GET"])
def fetch_top_repos():
    # Kiểm tra số lượng repos hiện có trong DB
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM repo")
    current_count = cursor.fetchone()[0]

    limit = request.args.get("limit", default=5000, type=int)

    # Nếu chưa đủ số lượng, chạy crawler
    if current_count < limit:
        process = CrawlerProcess()
        # process.crawl(GitHubReleasesSpider)
        process.crawl(RepoSpider, limit=limit)
        process.start()  # Chạy đồng bộ


    # Lấy dữ liệu từ DB
    cursor.execute("SELECT id, \"user\", name, star FROM repo LIMIT %s", (limit,))
    repos = [{"id": row[0], "user": row[1], "name": row[2], "full_name": f"{row[1]}/{row[2]}", "star": row[3]} for row in
             cursor.fetchall()]
    cursor.close()
    conn.close()

    return jsonify({"status": "success", "repos_count": len(repos), "repos": repos})


@app.route("/crawl-releases", methods=["POST"])
def crawl_releases():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM repo")
    if cursor.fetchone()[0] == 0:
        cursor.close()
        conn.close()
        return jsonify({"error": "No repos available. Call /top-repos first."}), 400

    cursor.close()
    conn.close()

    process = CrawlerProcess(settings={
        "USER_AGENT": "GitHubCrawler/1.0",
        "DOWNLOAD_DELAY": 1,
        "LOG_LEVEL": "INFO"
    })
    process.crawl(ReleasesSpider)
    process.start()
    return jsonify({"status": "crawl_completed"})


@app.route("/releases", methods=["GET"])
def get_releases():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT r.id, r.content, repo.user, repo.name
        FROM "release" r
        JOIN repo ON r.repoID = repo.id
    """)
    releases = [{"id": row[0], "content": json.loads(row[1]), "user": row[2], "name": row[3]} for row in
                cursor.fetchall()]
    cursor.close()
    conn.close()
    return jsonify({"status": "success", "releases": releases})


@app.route("/commits/<int:release_id>", methods=["GET"])
def get_commits(release_id):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT hash, message FROM \"commit\" WHERE releaseID = %s", (release_id,))
    commits = [{"hash": row[0], "message": row[1]} for row in cursor.fetchall()]
    cursor.close()
    conn.close()
    return jsonify({"status": "success", "release_id": release_id, "commits": commits})


def run_spider():
    global is_running
    if is_running:
        print("Đã có một spider đang chạy. Bỏ qua lần chạy này.")
        return

    try:
        is_running = True
        print("Bắt đầu chạy spider để thu thập dữ liệu từ GitHub...")
        process = CrawlerProcess()
        process.crawl(RepoSpider, limit=5000)  # Có thể thay đổi limit nếu cần
        process.start()
        print("Đã hoàn thành thu thập dữ liệu.")
    finally:
        is_running = False  # Đặt lại trạng thái sau khi hoàn thành


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5000)
    # schedule.every().minute.do(run_spider)
    #
    # # Ví dụ khác:
    # # schedule.every().minute.do(run_spider)  # Chạy mỗi phút (dùng để test)
    # # schedule.every().hour.do(run_spider)  # Chạy mỗi giờ
    #
    # print("Đã kích hoạt lịch trình. Đang chờ đến thời gian chạy tiếp theo...")
    # while True:
    #     schedule.run_pending()
    #     time.sleep(60)  # Kiểm tra mỗi phút