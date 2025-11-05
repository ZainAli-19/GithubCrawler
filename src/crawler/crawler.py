import os
import time
import logging
from crawler.graphql_client import GitHubClient

CHECKPOINT_FILE = "data/checkpoint.txt"
TOTAL_TARGET = 100_000  # Target number of repositories


def save_checkpoint(cursor_value, total_repos):
    os.makedirs("data", exist_ok=True)
    with open(CHECKPOINT_FILE, "w") as f:
        f.write(f"{cursor_value},{total_repos}")


def load_checkpoint():
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, "r") as f:
            content = f.read().strip().split(",")
            if len(content) == 2:
                cursor = content[0] if content[0] != "None" else None
                total = int(content[1])
                return cursor, total
    return None, 0


def crawl_repositories():
    logging.basicConfig(
        format="%(asctime)s | %(levelname)s | %(message)s", level=logging.INFO
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


if __name__ == "__main__":
    crawl_repositories()
