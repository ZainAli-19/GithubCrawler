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
MAX_RETRIES = 5
BATCH_SIZE = 100
WORKER_QUERIES = [
    "stars:>10000",
    "stars:5000..9999",
    "stars:1000..4999",
    "stars:100..999",
    "stars:1..99"
]

# ────────────────────────────────────────────────
# Utility: parse ISO datetime safely
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
    return await asyncpg.create_pool(
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        database=POSTGRES_DB,
        host=POSTGRES_HOST,
    )


async def insert_batch(pool, nodes):
    """Insert a batch of repository data into PostgreSQL."""
    async with pool.acquire() as conn:
        async with conn.transaction():
            for repo in nodes:
                # Convert timestamps
                created_at = parse_datetime(repo.get("createdAt"))
                updated_at = parse_datetime(repo.get("updatedAt"))
                pushed_at = parse_datetime(repo.get("pushedAt"))

                await conn.execute(
                    """
                    INSERT INTO repositories (id, name, owner, stars, forks, created_at, updated_at, pushed_at, url)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                    ON CONFLICT (id) DO NOTHING;
                    """,
                    repo["id"],
                    repo["name"],
                    repo["owner"]["login"] if repo.get("owner") else None,
                    repo["stargazerCount"],
                    repo["forkCount"],
                    created_at,
                    updated_at,
                    pushed_at,
                    repo["url"],
                )


# ────────────────────────────────────────────────
# Crawler logic
# ────────────────────────────────────────────────
async def crawl_slice(query: str, pool, client: GitHubGraphQLClient):
    """Crawl one slice of the repo space."""
    cursor = None
    page = 0

    for attempt in range(MAX_RETRIES):
        try:
            while True:
                data = await client.fetch_repos(query, cursor)
                if not data:
                    break

                nodes = data.get("nodes", [])
                if not nodes:
                    break

                await insert_batch(pool, nodes)
                page += 1

                page_info = data.get("pageInfo", {})
                if not page_info.get("hasNextPage"):
                    break
                cursor = page_info.get("endCursor")

                logging.info(f"🧵 Worker {query} page {page} complete ({len(nodes)} repos)")

            logging.info(f"✅ Worker {query} finished ({page} pages)")
            break

        except Exception as e:
            logging.error(f"⚠️ Worker {query} failed attempt {attempt+1}: {e}")
            await asyncio.sleep(2 ** attempt)
    else:
        logging.error(f"❌ Worker {query} exhausted retries.")


# ────────────────────────────────────────────────
# Main entrypoint
# ────────────────────────────────────────────────
async def main():
    logging.info("🚀 Starting async GitHub crawler...")

    async with aiohttp.ClientSession() as session:
        client = GitHubGraphQLClient(GITHUB_TOKEN, session)
        pool = await init_db_pool()

        tasks = [crawl_slice(q, pool, client) for q in WORKER_QUERIES]
        await asyncio.gather(*tasks)

        await pool.close()

    logging.info("✅ Parallel crawl completed successfully.")


if __name__ == "__main__":
    asyncio.run(main())
