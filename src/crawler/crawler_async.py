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
TOTAL_TARGET = 100_000
BATCH_SIZE = 100
WORKERS = 5
MAX_RETRIES = 5
BASE_QUERIES = [
    "stars:>10000",
    "stars:5000..9999",
    "stars:1000..4999",
    "stars:100..999",
    "stars:1..99",
]

# ────────────────────────────────────────────────
# Utils
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
# DB helpers
# ────────────────────────────────────────────────
async def init_db_pool():
    return await asyncpg.create_pool(
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        database=POSTGRES_DB,
        host=POSTGRES_HOST,
        min_size=1,
        max_size=10,
    )


async def insert_batch(pool, nodes):
    """Insert batch safely into DB."""
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
                    logging.warning(f"⚠️ DB insert skip: {e}")


async def get_repo_count(pool):
    async with pool.acquire() as conn:
        val = await conn.fetchval("SELECT COUNT(*) FROM repositories;")
        return val or 0


# ────────────────────────────────────────────────
# Crawl logic
# ────────────────────────────────────────────────
async def crawl_range(query: str, pool, client, stop_event: asyncio.Event):
    """Crawl a given search query until no pages left or stop_event is set."""
    cursor = None
    total = 0
    page = 0
    while not stop_event.is_set():
        try:
            data = await client.fetch_repos(query, cursor)
            if not data:
                break
            nodes = data.get("nodes", [])
            if not nodes:
                break

            await insert_batch(pool, nodes)
            total += len(nodes)
            page += 1
            logging.info(f"📦 [{query}] page {page}, total {total}")

            page_info = data.get("pageInfo", {})
            if not page_info.get("hasNextPage"):
                break
            cursor = page_info.get("endCursor")

            await asyncio.sleep(1.2)
        except Exception as e:
            logging.warning(f"⚠️ Range [{query}] failed: {e}")
            await asyncio.sleep(3)
    return total


async def crawl_worker(base_query: str, pool, client, stop_event: asyncio.Event, worker_id: int):
    """One worker dynamically exploring sub-ranges."""
    step = 100  # dynamic subrange star step (used only for <1000 ranges)
    total = 0
    logging.info(f"🧵 Worker {worker_id} starting on {base_query}")

    # Expand low-star queries to cover more repos
    if base_query.startswith("stars:1.."):
        low, high = 1, 999
        while not stop_event.is_set() and low < high:
            sub_q = f"stars:{low}..{low+step-1}"
            got = await crawl_range(sub_q, pool, client, stop_event)
            total += got
            low += step
            count = await get_repo_count(pool)
            logging.info(f"Worker {worker_id}: DB={count}, subrange={sub_q}")
            if count >= TOTAL_TARGET:
                stop_event.set()
                break
    else:
        await crawl_range(base_query, pool, client, stop_event)

    logging.info(f"🏁 Worker {worker_id} done — crawled ~{total} repos.")


# ────────────────────────────────────────────────
# Entrypoint
# ────────────────────────────────────────────────
async def main():
    logging.info(f"🚀 Starting async GitHub crawler aiming for {TOTAL_TARGET} repos...")

    if not GITHUB_TOKEN:
        raise ValueError("❌ Missing GITHUB_TOKEN environment variable.")

    async with aiohttp.ClientSession() as session:
        client = GitHubGraphQLClient(GITHUB_TOKEN, session)
        pool = await init_db_pool()

        stop_event = asyncio.Event()
        tasks = [
            asyncio.create_task(crawl_worker(q, pool, client, stop_event, i + 1))
            for i, q in enumerate(BASE_QUERIES)
        ]

        # Watch DB count concurrently
        async def watcher():
            while not stop_event.is_set():
                count = await get_repo_count(pool)
                if count >= TOTAL_TARGET:
                    logging.info(f"🎯 Target reached: {count} repos.")
                    stop_event.set()
                    break
                await asyncio.sleep(30)

        tasks.append(asyncio.create_task(watcher()))
        await asyncio.gather(*tasks)
        await pool.close()

    logging.info("🎉 Crawl complete — database has ≥100 000 repositories.")


if __name__ == "__main__":
    asyncio.run(main())
