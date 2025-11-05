import os
import psycopg2
import time
import logging
from crawler.graphql_client import GitHubClient

# ===============================
# Logging Configuration
# ===============================
logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(message)s",
    level=logging.INFO,
)

# ===============================
# Environment Configuration
# ===============================
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_DB = os.getenv("POSTGRES_DB", "postgres")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")

# ===============================
# GraphQL Query
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
# Main Crawler Logic
# ===============================
def crawl():
    client = GitHubClient(GITHUB_TOKEN)

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
        data = client.run_query(query_template, {"cursor": cursor_value})
        if not data or "data" not in data or "search" not in data["data"]:
            logging.warning("⚠️ Empty or invalid response, retrying...")
            time.sleep(5)
            continue

        repos = data["data"]["search"]["nodes"]
        page_info = data["data"]["search"]["pageInfo"]

        insert_repo_batch(cursor, repos)
        connection.commit()

        total_repos += len(repos)
        logging.info(f"✅ Inserted {total_repos} repositories so far...")

        if not page_info["hasNextPage"]:
            logging.info("🎉 No more pages available — crawl complete.")
            break

        cursor_value = page_info["endCursor"]
        time.sleep(1.5)

    cursor.close()
    connection.close()
    logging.info("🎉 Crawl completed successfully!")

# ===============================
# Run Script
# ===============================
if __name__ == "__main__":
    crawl()
