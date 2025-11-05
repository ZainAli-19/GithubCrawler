import os
import requests
import psycopg2
import time
import logging
from datetime import datetime

# ===============================
# Logging Configuration
# ===============================
logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(message)s",
    level=logging.INFO,
)

# ===============================
# GitHub Crawler Configuration
# ===============================
GITHUB_API_URL = "https://api.github.com/graphql"
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_DB = os.getenv("POSTGRES_DB", "postgres")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")

# ===============================
# GraphQL Query (Fixed)
# ===============================
query_template = """
query($cursor: String) {
  search(query: "stars:>100", type: REPOSITORY, first: 100, after: $cursor) {
    pageInfo {
      hasNextPage
      endCursor
    }
    nodes {
      ... on Repository {
        id
        name
        nameWithOwner
        stargazerCount
        primaryLanguage { name }
        createdAt
        updatedAt
        owner { login }
      }
    }
  }
}
"""

# ===============================
# Database Insertion Logic
# ===============================
def insert_repo_batch(cursor, repos):
    for repo in repos:
        cursor.execute(
            """
            INSERT INTO repositories (id, name, full_name, stars, language, created_at, updated_at, owner)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (id) DO UPDATE
            SET stars = EXCLUDED.stars,
                updated_at = EXCLUDED.updated_at;
            """,
            (
                repo["id"],
                repo["name"],
                repo["nameWithOwner"],
                repo["stargazerCount"],
                repo["primaryLanguage"]["name"] if repo["primaryLanguage"] else None,
                repo["createdAt"],
                repo["updatedAt"],
                repo["owner"]["login"],
            ),
        )

# ===============================
# Helper: GitHub API with retry & rate-limit
# ===============================
def github_post(query, variables, headers, max_retries=5):
    for attempt in range(max_retries):
        response = requests.post(GITHUB_API_URL, json={"query": query, "variables": variables}, headers=headers)
        if response.status_code == 200:
            data = response.json()
            if "errors" not in data:
                return data
            logging.warning(f"GraphQL errors: {data['errors']}")
        elif response.status_code == 502:
            logging.warning("Received 502 Bad Gateway, retrying...")
        elif response.status_code == 403 and "X-RateLimit-Reset" in response.headers:
            reset_time = int(response.headers["X-RateLimit-Reset"])
            wait_time = max(0, reset_time - int(time.time())) + 5
            logging.warning(f"Rate limit hit. Waiting {wait_time} seconds...")
            time.sleep(wait_time)
            continue

        time.sleep(2 ** attempt)  # exponential backoff
    logging.error(f"Failed after {max_retries} retries.")
    return None

# ===============================
# Main Crawler Logic
# ===============================
def crawl():
    headers = {"Authorization": f"Bearer {GITHUB_TOKEN}"}
    connection = psycopg2.connect(
        host=POSTGRES_HOST,
        database=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
    )
    cursor = connection.cursor()

    total_repos = 0
    cursor_value = None

    logging.info("🚀 Starting crawl of GitHub repositories...")

    while total_repos < 100_000:
        data = github_post(query_template, {"cursor": cursor_value}, headers)
        if not data:
            logging.warning("No data received. Retrying...")
            time.sleep(5)
            continue

        repos = data["data"]["search"]["nodes"]
        page_info = data["data"]["search"]["pageInfo"]

        insert_repo_batch(cursor, repos)
        connection.commit()

        total_repos += len(repos)
        logging.info(f"✅ Inserted {total_repos} repositories so far...")

        if not page_info["hasNextPage"]:
            break

        cursor_value = page_info["endCursor"]
        time.sleep(1.5)  # Gentle pause for rate limits

    cursor.close()
    connection.close()
    logging.info("🎉 Crawl completed successfully!")

# ===============================
# Run Script
# ===============================
if __name__ == "__main__":
    crawl()
