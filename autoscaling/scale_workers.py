import logging
import math
import shlex
import subprocess
import requests

# Configuration
PROMETHEUS_URL = "http://localhost:9090/api/v1/query"  # Prometheus API endpoint
REDIS_KEY_QUERY = 'redis_key_size{db="db0",key="repos:requests"}'
DOCKER_COMPOSE_FILE = "/home/lenovo/PycharmProjects/github-crawler/docker-compose.yml"
SERVICE_NAME = "release-commit-worker"  # Service name in docker-compose.yml

def get_queue_size():
    try:
        # Make API call to Prometheus
        response = requests.get(PROMETHEUS_URL, params={"query": REDIS_KEY_QUERY})
        print(f"Response: {response.status_code} - {response.text}")

        # Check if the request was successful
        if response.status_code == 200:
            data = response.json()
            if data["status"] == "success" and data["data"]["result"]:
                # Extract the queue size from the response
                queue_size = int(float(data["data"]["result"][0]["value"][1]))
                return queue_size
        print("No data returned or invalid response from Prometheus")
        return 0
    except Exception as e:
        print(f"Error querying Prometheus: {e}")
        return 0

def get_current_workers():
    try:
        # Lệnh lấy ID container của service đang chạy
        cmd = f"docker compose -f {shlex.quote(DOCKER_COMPOSE_FILE)} ps -q {shlex.quote(SERVICE_NAME)} | sort -u | wc -l"
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True, check=True)
        return int(result.stdout.strip())
    except subprocess.CalledProcessError as e:
        print(f"Error running docker compose command: {e}, Output: {e.output}")
        return 0
    except Exception as e:
        print(f"Error getting current workers: {e}")
        return 0

def scale_workers(queue_size):
    # Calculate desired number of workers: 1000 queue size = 1 worker
    desired_workers = max(1, math.ceil(queue_size / 1000))
    current_workers = get_current_workers()

    print(f"Queue size: {queue_size}, Current workers: {current_workers}, Desired workers: {desired_workers}")

    if desired_workers == current_workers:
        print("No scaling needed")
        return

    if desired_workers > current_workers:
        print(f"Scaling up from {current_workers} to {desired_workers} workers")
    else:
        print(f"Scaling down from {current_workers} to {desired_workers} workers")

    # Update scale in Docker Compose
    cmd = f"docker compose -f {DOCKER_COMPOSE_FILE} up -d {SERVICE_NAME} --scale {SERVICE_NAME}={desired_workers}"
    subprocess.run(cmd, shell=True, check=True)

def main():
    queue_size = get_queue_size()
    scale_workers(queue_size)

if __name__ == "__main__":
    main()