import os
import asyncio
import logging
from datetime import datetime
import aiohttp
import asyncpg
from crawler.graphql_client import GitHubGraphQLClient

# ────────────────────────────────────────────────
# Logging setup
# ────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(message)s",
)

# ────────────────────────────────────────────────
# Environment variables
# ────────────────────────────────────────────────
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")
POSTGRES_DB = os.getenv("POSTGRES_DB", "postgres")
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")

# ────────────────────────────────────────────────
# Constants
# ────────────────────────────────────────────────
TOTAL_TARGET = 100_000  # total repos to collect
BATCH_SIZE = 100
WORKERS = 5
MAX_RETRIES = 5

# Each worker crawls a specific star range (for diversity)
WORKER_QUERIES = [
    "stars:>10000",
    "stars:5000..9999",
    "stars:1000..4999",
    "stars:100..999",
    "stars:1..99"
]


# ────────────────────────────────────────────────
# Utility functions
# ────────────────────────────────────────────────
def parse_datetime(ts: str):
    """Convert ISO8601 string to datetime.datetime or return None."""
    if ts and isinstance(ts, str):
        for fmt in ("%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S.%fZ"):
            try:
                return datetime.strptime(ts, fmt)
            except ValueError:
                continue
    return None


# ────────────────────────────────────────────────
# Database helpers
# ────────────────────────────────────────────────
async def init_db_pool():
    """Initialize PostgreSQL connection pool."""
    return await asyncpg.create_pool(
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        database=POSTGRES_DB,
        host=POSTGRES_HOST,
        min_size=1,
        max_size=10,
    )


async def insert_batch(pool, nodes):
    """Insert a batch of repositories into PostgreSQL."""
    async with pool.acquire() as conn:
        async with conn.transaction():
            for repo in nodes:
                try:
                    await conn.execute(
                        """
                        INSERT INTO repositories (
                            id, name, owner, stars, forks,
                            created_at, updated_at, pushed_at, url
                        )
                        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
                        ON CONFLICT (id) DO UPDATE
                        SET stars = EXCLUDED.stars,
                            forks = EXCLUDED.forks,
                            updated_at = EXCLUDED.updated_at;
                        """,
                        repo["id"],
                        repo["name"],
                        repo["owner"]["login"] if repo.get("owner") else None,
                        repo["stargazerCount"],
                        repo["forkCount"],
                        parse_datetime(repo.get("createdAt")),
                        parse_datetime(repo.get("updatedAt")),
                        parse_datetime(repo.get("pushedAt")),
                        repo["url"],
                    )
                except Exception as e:
                    logging.warning(f"⚠️ Skipped repo due to DB error: {e}")


# ────────────────────────────────────────────────
# Worker Task
# ────────────────────────────────────────────────
async def crawl_slice(query: str, pool, client: GitHubGraphQLClient, target_per_worker: int):
    """Crawl one slice of the repository space."""
    cursor = None
    total = 0
    pages = 0

    logging.info(f"🧵 Starting worker for query: {query}")

    for attempt in range(MAX_RETRIES):
        try:
            while total < target_per_worker:
                data = await client.fetch_repos(query, cursor)
                if not data:
                    logging.warning(f"⚠️ Empty response for {query}, retrying...")
                    await asyncio.sleep(3)
                    continue

                nodes = data.get("nodes", [])
                if not nodes:
                    break

                await insert_batch(pool, nodes)
                total += len(nodes)
                pages += 1

                logging.info(f"📦 Worker [{query}] — page {pages}, total {total}")

                page_info = data.get("pageInfo", {})
                if not page_info.get("hasNextPage"):
                    logging.info(f"✅ Worker [{query}] finished (no more pages).")
                    break

                cursor = page_info.get("endCursor")

                # Respect GitHub API rate limits
                await asyncio.sleep(1.2)

            break  # success, stop retrying
        except Exception as e:
            logging.error(f"❌ Worker [{query}] failed attempt {attempt+1}: {e}")
            await asyncio.sleep(2 ** attempt)
    else:
        logging.error(f"❌ Worker [{query}] gave up after {MAX_RETRIES} retries.")

    logging.info(f"🏁 Worker [{query}] completed with {total} repos.")


# ────────────────────────────────────────────────
# Main Entrypoint
# ────────────────────────────────────────────────
async def main():
    logging.info("🚀 Starting async GitHub crawler for 100,000 repositories...")

    if not GITHUB_TOKEN:
        raise ValueError("❌ Missing GITHUB_TOKEN environment variable.")

    async with aiohttp.ClientSession() as session:
        client = GitHubGraphQLClient(GITHUB_TOKEN, session)
        pool = await init_db_pool()

        target_per_worker = TOTAL_TARGET // WORKERS

        # Launch all worker tasks
        tasks = [
            asyncio.create_task(crawl_slice(q, pool, client, target_per_worker))
            for q in WORKER_QUERIES
        ]

        await asyncio.gather(*tasks)
        await pool.close()

    logging.info("🎉 All workers finished — crawl complete (≈100,000 repos).")


if __name__ == "__main__":
    asyncio.run(main())
