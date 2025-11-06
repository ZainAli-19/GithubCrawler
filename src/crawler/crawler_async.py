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
WORKERS = 8  # ⚡ increased concurrency safely
INSERT_BATCH_SIZE = 300  # group inserts for fewer transactions
RATE_DELAY = 0.8  # reduced sleep (still API-safe)
MAX_RETRIES = 5

# Diverse star ranges to distribute load across GitHub’s data clusters
BASE_QUERIES = [
    "stars:>20000",
    "stars:10000..19999",
    "stars:5000..9999",
    "stars:1000..4999",
    "stars:500..999",
    "stars:200..499",
    "stars:100..199",
    "stars:1..99",
]

# ────────────────────────────────────────────────
# Utils
# ────────────────────────────────────────────────
def parse_datetime(ts: str):
    if not ts:
        return None
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
        min_size=2,
        max_size=20,
    )


async def insert_batch(pool, nodes):
    """Efficient bulk insert for performance."""
    if not nodes:
        return
    async with pool.acquire() as conn:
        async with conn.transaction():
            values = [
                (
                    n["id"],
                    n["name"],
                    n["owner"]["login"] if n.get("owner") else None,
                    n.get("stargazerCount", 0),
                    n.get("forkCount", 0),
                    parse_datetime(n.get("createdAt")),
                    parse_datetime(n.get("updatedAt")),
                    parse_datetime(n.get("pushedAt")),
                    n.get("url"),
                )
                for n in nodes
            ]
            try:
                await conn.executemany(
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
                    values,
                )
            except Exception as e:
                logging.warning(f"⚠️ Bulk insert failed: {e}")


async def get_repo_count(pool):
    async with pool.acquire() as conn:
        return await conn.fetchval("SELECT COUNT(*) FROM repositories;") or 0


# ────────────────────────────────────────────────
# Crawl logic
# ────────────────────────────────────────────────
async def crawl_range(query, pool, client, stop_event):
    cursor = None
    total = 0
    buffer = []
    while not stop_event.is_set():
        try:
            data = await client.fetch_repos(query, cursor)
            if not data:
                break

            nodes = data.get("nodes", [])
            if not nodes:
                break

            buffer.extend(nodes)
            if len(buffer) >= INSERT_BATCH_SIZE:
                await insert_batch(pool, buffer)
                total += len(buffer)
                buffer.clear()
                logging.info(f"📦 [{query}] Inserted {total:,} so far")

            page_info = data.get("pageInfo", {})
            if not page_info.get("hasNextPage"):
                break
            cursor = page_info.get("endCursor")
            await asyncio.sleep(RATE_DELAY)
        except Exception as e:
            logging.warning(f"⚠️ Crawl error in {query}: {e}")
            await asyncio.sleep(2)

    # Insert remaining items
    if buffer:
        await insert_batch(pool, buffer)
        total += len(buffer)
    return total


async def crawl_worker(base_query, pool, client, stop_event, worker_id):
    logging.info(f"🧵 Worker-{worker_id} → {base_query}")
    total = await crawl_range(base_query, pool, client, stop_event)
    count = await get_repo_count(pool)
    logging.info(f"🏁 Worker-{worker_id} done ({total:,} repos, DB={count:,})")
    if count >= TOTAL_TARGET:
        stop_event.set()


# ────────────────────────────────────────────────
# Entrypoint
# ────────────────────────────────────────────────
async def main():
    logging.info(f"🚀 Starting async GitHub crawler (target={TOTAL_TARGET:,})")

    if not GITHUB_TOKEN:
        raise ValueError("❌ Missing GITHUB_TOKEN environment variable")

    pool = await init_db_pool()

    async with aiohttp.ClientSession() as session:
        client = GitHubGraphQLClient(GITHUB_TOKEN, session)
        stop_event = asyncio.Event()

        workers = [
            asyncio.create_task(crawl_worker(q, pool, client, stop_event, i + 1))
            for i, q in enumerate(BASE_QUERIES)
        ]

        # watcher to stop early
        async def watcher():
            while not stop_event.is_set():
                count = await get_repo_count(pool)
                logging.info(f"📊 DB count: {count:,}")
                if count >= TOTAL_TARGET:
                    stop_event.set()
                    break
                await asyncio.sleep(20)

        workers.append(asyncio.create_task(watcher()))
        await asyncio.gather(*workers)

    await pool.close()
    logging.info("🎉 Crawl complete — database has ≥100,000 repositories.")


if __name__ == "__main__":
    asyncio.run(main())
