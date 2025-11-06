import os
import time
import logging
import psycopg2
from psycopg2.extras import execute_values
from crawler.graphql_client import GitHubClient

CHECKPOINT_FILE = "data/checkpoint.txt"
TOTAL_TARGET = 100_000  # Target number of repositories


# ==========================
# PostgreSQL Connection
# ==========================
def get_connection(retries=10, delay=5):
    """Connect to PostgreSQL with auto-retry."""
    for attempt in range(1, retries + 1):
        try:
            conn = psycopg2.connect(
                host=os.getenv("POSTGRES_HOST", "localhost"),
                database=os.getenv("POSTGRES_DB", "postgres"),
                user=os.getenv("POSTGRES_USER", "postgres"),
                password=os.getenv("POSTGRES_PASSWORD", "postgres"),
            )
            conn.autocommit = False
            logging.info("✅ Connected to PostgreSQL.")
            return conn
        except psycopg2.OperationalError as e:
            logging.warning(f"⚠️ DB connection failed (attempt {attempt}/{retries}): {e}")
            time.sleep(delay)
    raise RuntimeError("❌ Could not connect to PostgreSQL after multiple attempts.")


# ==========================
# Checkpoint Handling (File)
# ==========================
def save_checkpoint_file(cursor_value, total_repos):
    os.makedirs("data", exist_ok=True)
    with open(CHECKPOINT_FILE, "w") as f:
        f.write(f"{cursor_value},{total_repos}")


def load_checkpoint_file():
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, "r") as f:
            content = f.read().strip().split(",")
            if len(content) == 2:
                cursor = content[0] if content[0] != "None" else None
                total = int(content[1])
                return cursor, total
    return None, 0


# ==========================
# Checkpoint Handling (DB)
# ==========================
def save_checkpoint_db(conn, cursor_value, total_repos):
    """Save progress inside crawl_checkpoint table."""
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO crawl_checkpoint (cursor_value, total_repos, updated_at)
                VALUES (%s, %s, NOW())
                ON CONFLICT (id) DO UPDATE
                SET cursor_value = EXCLUDED.cursor_value,
                    total_repos = EXCLUDED.total_repos,
                    updated_at = NOW();
            """, (cursor_value, total_repos))
        conn.commit()
    except Exception as e:
        conn.rollback()
        logging.warning(f"⚠️ Could not save checkpoint to DB: {e}")


def load_checkpoint_db(conn):
    """Load checkpoint from DB, fallback to file if empty."""
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT cursor_value, total_repos FROM crawl_checkpoint ORDER BY id DESC LIMIT 1;")
            row = cur.fetchone()
            if row:
                return row[0], row[1]
    except Exception as e:
        logging.warning(f"⚠️ Could not load checkpoint from DB: {e}")
    return load_checkpoint_file()


# ==========================
# Repository Insertion
# ==========================
def insert_repositories(conn, edges):
    """Batch insert repos with ON CONFLICT handling and auto-reconnect."""
    if not edges:
        return

    repos_data = []
    for edge in edges:
        repo = edge["node"]
        repos_data.append((
            repo["url"],  # used for hash id
            repo["name"],
            f"{repo['owner']['login']}/{repo['name']}",
            repo["stargazerCount"],
            repo["primaryLanguage"]["name"] if repo["primaryLanguage"] else None,
            repo["createdAt"],
            repo["updatedAt"],
            repo["owner"]["login"]
        ))

    query = """
    INSERT INTO repositories (id, name, full_name, stars, language, created_at, updated_at, owner)
    VALUES (md5(%s::text), %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (id) DO UPDATE
    SET stars = EXCLUDED.stars,
        updated_at = EXCLUDED.updated_at;
    """

    for attempt in range(3):
        try:
            with conn.cursor() as cur:
                execute_values(cur, query, repos_data, page_size=100)
            conn.commit()
            logging.info(f"🧩 Inserted batch of {len(repos_data)} repositories.")
            return
        except psycopg2.OperationalError as e:
            logging.warning(f"⚠️ Lost DB connection (attempt {attempt+1}/3): {e}")
            conn = get_connection()
        except Exception as e:
            conn.rollback()
            logging.error(f"❌ Insert failed: {e}")
            time.sleep(2)
    logging.critical("❌ All insert attempts failed.")


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

    cursor_value, total_repos = load_checkpoint_db(conn)
    logging.info(f"🚀 Starting/resuming crawl — {total_repos} repos already collected.")

    while total_repos < TOTAL_TARGET:
        data = client.run_query(query_template, {"cursor": cursor_value})
        if not data or "data" not in data:
            logging.warning("⚠️ No data — retrying in 30s...")
            time.sleep(30)
            continue

        edges = data["data"]["search"]["edges"]
        if not edges:
            logging.info("🎉 No more pages available.")
            break

        insert_repositories(conn, edges)
        total_repos += len(edges)

        page_info = data["data"]["search"]["pageInfo"]
        cursor_value = page_info["endCursor"]

        # Save checkpoint in both file & DB
        save_checkpoint_file(cursor_value, total_repos)
        save_checkpoint_db(conn, cursor_value, total_repos)

        logging.info(f"✅ Total repositories inserted: {total_repos}")

        if not page_info["hasNextPage"]:
            logging.info("🎉 Crawl completed successfully!")
            break

        time.sleep(2)  # polite delay

    conn.close()
    logging.info("🔚 Crawl finished — PostgreSQL connection closed.")


if __name__ == "__main__":
    crawl_repositories()
