import os
import psycopg2
import time
import logging
from datetime import datetime
from crawler.graphql_client import GitHubClient

logging.basicConfig(format="%(asctime)s | %(levelname)s | %(message)s", level=logging.INFO)

# ===============================
# Environment Config
# ===============================
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_DB = os.getenv("POSTGRES_DB", "postgres")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")

# ===============================
# Query Template
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
# DB Helpers
# ===============================
def get_connection():
    return psycopg2.connect(
        host=POSTGRES_HOST,
        database=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
    )

def get_state(cursor):
    cursor.execute("SELECT last_cursor, total_repos FROM crawl_state ORDER BY id DESC LIMIT 1;")
    row = cursor.fetchone()
    return row if row else (None, 0)

def save_state(cursor, last_cursor, total_repos):
    cursor.execute(
        """
        INSERT INTO crawl_state (last_cursor, total_repos, updated_at)
        VALUES (%s, %s, %s);
        """,
        (last_cursor, total_repos, datetime.utcnow()),
    )

# ===============================
# Insert Repositories
# ===============================
def insert_repo_batch(cursor, repos):
    params = [
        (
            r["id"],
            r["name"],
            r["nameWithOwner"],
            r["stargazerCount"],
            r["primaryLanguage"]["name"] if r["primaryLanguage"] else None,
            r["createdAt"],
            r["updatedAt"],
            r["owner"]["login"],
        )
        for r in repos
    ]
    cursor.executemany(
        """
        INSERT INTO repositories (id, name, full_name, stars, language, created_at, updated_at, owner)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (id) DO UPDATE
        SET stars = EXCLUDED.stars, updated_at = EXCLUDED.updated_at;
        """,
        params,
    )

# ===============================
# Crawl Logic
# ===============================
def crawl():
    client = GitHubClient(GITHUB_TOKEN)
    conn = get_connection()
    cur = conn.cursor()

    cursor_value, total_repos = get_state(cur)
    logging.info(f"🚀 Resuming crawl — already have {total_repos} repos.")

    while total_repos < 100_000:
        try:
            data = client.run_query(query_template, {"cursor": cursor_value})
            if not data or "data" not in data or "search" not in data["data"]:
                logging.warning("⚠️ Invalid response, retrying in 10s...")
                time.sleep(10)
                continue

            repos = data["data"]["search"]["nodes"]
            page_info = data["data"]["search"]["pageInfo"]

            insert_repo_batch(cur, repos)
            total_repos += len(repos)
            save_state(cur, page_info["endCursor"], total_repos)
            conn.commit()

            logging.info(f"✅ Inserted {total_repos} repositories so far...")

            if not page_info["hasNextPage"]:
                logging.info("🎉 No more pages — crawl complete.")
                break

            cursor_value = page_info["endCursor"]
            time.sleep(1.5)

        except Exception as e:
            logging.error(f"❌ Error: {e}. Retrying in 30s...")
            time.sleep(30)
            continue

    cur.close()
    conn.close()
    logging.info("🎉 Crawl completed successfully!")

# ===============================
# Run
# ===============================
if __name__ == "__main__":
    crawl()
