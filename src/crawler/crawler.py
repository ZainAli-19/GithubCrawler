import os
import time
import logging
import psycopg2
import hashlib
from crawler.graphql_client import GitHubClient

# ==========================
# Configuration
# ==========================
CHECKPOINT_FILE = "data/checkpoint.txt"
TOTAL_TARGET = 100_000  # Target number of repositories


# ==========================
# PostgreSQL Connection
# ==========================
def get_connection(retries=10, delay=5):
    """Establish and return a PostgreSQL connection with retries."""
    for attempt in range(1, retries + 1):
        try:
            conn = psycopg2.connect(
                host=os.getenv("POSTGRES_HOST", "localhost"),
                database=os.getenv("POSTGRES_DB", "postgres"),
                user=os.getenv("POSTGRES_USER", "postgres"),
                password=os.getenv("POSTGRES_PASSWORD", "postgres"),
            )
            logging.info("✅ Connected to PostgreSQL.")
            return conn
        except psycopg2.OperationalError as e:
            logging.warning(f"⚠️ Database not ready (attempt {attempt}/{retries}): {e}")
            time.sleep(delay)
    raise RuntimeError("❌ Could not connect to PostgreSQL after multiple attempts.")


# ==========================
# Checkpoint Handling
# ==========================
def save_checkpoint(cursor_value, total_repos):
    """Save progress to file and database (if available)."""
    os.makedirs("data", exist_ok=True)
    with open(CHECKPOINT_FILE, "w") as f:
        f.write(f"{cursor_value},{total_repos}")

    try:
        conn = get_connection()
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO crawl_checkpoint (id, cursor_value, total_repos, updated_at)
                VALUES (1, %s, %s, NOW())
                ON CONFLICT (id) DO UPDATE
                SET cursor_value = EXCLUDED.cursor_value,
                    total_repos = EXCLUDED.total_repos,
                    updated_at = NOW();
            """, (cursor_value, total_repos))
            conn.commit()
        conn.close()
    except Exception as e:
        logging.warning(f"⚠️ Could not save checkpoint to DB: {e}")


def load_checkpoint():
    """Load progress from database first, fallback to file."""
    try:
        conn = get_connection()
        with conn.cursor() as cur:
            cur.execute("SELECT cursor_value, total_repos FROM crawl_checkpoint WHERE id = 1;")
            row = cur.fetchone()
        conn.close()
        if row:
            cursor, total = row
            return cursor, total
    except Exception:
        pass

    # fallback to local file
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, "r") as f:
            content = f.read().strip().split(",")
            if len(content) == 2:
                cursor = content[0] if content[0] != "None" else None
                total = int(content[1])
                return cursor, total
    return None, 0


# ==========================
# Repository Insertion
# ==========================
def insert_repositories(conn, edges):
    """Insert a batch of repositories into the database."""
    with conn.cursor() as cur:
        for edge in edges:
            repo = edge["node"]
            unique_id = hashlib.md5(repo["url"].encode("utf-8")).hexdigest()

            try:
                cur.execute(
                    """
                    INSERT INTO repositories (id, name, full_name, stars, language, created_at, updated_at, owner)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (id) DO UPDATE
                    SET stars = EXCLUDED.stars,
                        updated_at = EXCLUDED.updated_at;
                    """,
                    (
                        unique_id,
                        repo["name"],
                        f"{repo['owner']['login']}/{repo['name']}",
                        repo["stargazerCount"],
                        repo["primaryLanguage"]["name"] if repo["primaryLanguage"] else None,
                        repo["createdAt"],
                        repo["updatedAt"],
                        repo["owner"]["login"],
                    ),
                )
            except Exception as e:
                logging.error(f"❌ Insert failed for {repo['url']}: {e}")

    conn.commit()


# ==========================
# Main Crawler Logic
# ==========================
def crawl_repositories():
    logging.basicConfig(
        format="%(asctime)s | %(levelname)s | %(message)s",
        level=logging.INFO
    )

    token = os.getenv("GITHUB_TOKEN")
    if not token:
        raise ValueError("❌ Missing GITHUB_TOKEN environment variable.")

    client = GitHubClient(token)
    conn = get_connection()

    query_template = """
    query ($cursor: String) {
      search(query: "stars:>1 sort:stars-desc", type: REPOSITORY, first: 100, after: $cursor) {
        edges {
          node {
            ... on Repository {
              name
              owner { login }
              stargazerCount
              forkCount
              createdAt
              updatedAt
              url
              primaryLanguage { name }
            }
          }
        }
        pageInfo {
          endCursor
          hasNextPage
        }
      }
    }
    """

    cursor_value, total_repos = load_checkpoint()
    logging.info(f"🚀 Resuming crawl — already have {total_repos} repos.")

    while total_repos < TOTAL_TARGET:
        data = client.run_query(query_template, {"cursor": cursor_value})
        if not data or "data" not in data:
            logging.warning("⚠️ No data returned. Sleeping 30s before retry...")
            time.sleep(30)
            continue

        edges = data["data"]["search"]["edges"]
        if not edges:
            logging.info("🎉 No more pages — crawl complete.")
            break

        insert_repositories(conn, edges)
        total_repos += len(edges)

        page_info = data["data"]["search"]["pageInfo"]
        cursor_value = page_info["endCursor"]
        save_checkpoint(cursor_value, total_repos)

        logging.info(f"✅ Inserted {total_repos} repositories so far...")

        if not page_info["hasNextPage"]:
            logging.info("🎉 Crawl completed successfully!")
            break

        time.sleep(2)  # small delay to respect GitHub API rate limits

    conn.close()
    logging.info("🔚 Crawl finished — database connection closed.")


# ==========================
# Entry Point
# ==========================
if __name__ == "__main__":
    crawl_repositories()
