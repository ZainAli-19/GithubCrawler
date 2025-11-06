import os
import asyncio
import logging
import random
from datetime import datetime, timezone
import aiohttp
import asyncpg
from crawler.graphql_client import GitHubGraphQLClient
import math
from asyncio import Queue
import itertools


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(message)s",
)

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")
POSTGRES_DB = os.getenv("POSTGRES_DB", "postgres")
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")

TOTAL_TARGET = 100_000
WORKERS = 5                   
INSERT_BATCH_SIZE = 300
RATE_DELAY = 2.0               
MAX_BACKOFF = 10

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

RATE_LOCK = asyncio.Semaphore(1)

def parse_datetime(ts: str):
    if not ts:
        return None
    for fmt in ("%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S.%fZ"):
        try:
            return datetime.strptime(ts, fmt)
        except ValueError:
            continue
    return None


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
                logging.warning(f"Bulk insert failed: {e}")


async def get_repo_count(pool):
    async with pool.acquire() as conn:
        val = await conn.fetchval("SELECT COUNT(*) FROM repositories;")
        return val or 0


async def crawl_range(query, pool, client, stop_event):
    cursor = None
    total = 0
    buffer = []

    while not stop_event.is_set():
        try:
            
            async with RATE_LOCK:
                await asyncio.sleep(RATE_DELAY + random.uniform(0.5, 1.5))
                data = await client.fetch_repos(query, cursor)

            if not data:
                logging.warning(f"Empty response for {query}, waiting 10s...")
                await asyncio.sleep(10)
                continue

            nodes = data.get("nodes", [])
            page_info = data.get("pageInfo", {})

            if nodes:
                buffer.extend(nodes)
                if len(buffer) >= INSERT_BATCH_SIZE:
                    await insert_batch(pool, buffer)
                    total += len(buffer)
                    buffer.clear()
                    logging.info(f"[{query}] Inserted {total:,} so far")

            if not page_info.get("hasNextPage"):
                break
            cursor = page_info.get("endCursor")

        except asyncio.CancelledError:
            break
        except Exception as e:
            logging.warning(f"Crawl error in {query}: {e}")
            await asyncio.sleep(random.uniform(5, MAX_BACKOFF))

    if buffer:
        await insert_batch(pool, buffer)
        total += len(buffer)

    return total


# --- Query pool generation for maximum diversity ---
SUPPORTED_LANGUAGES = [
    'Python', 'JavaScript', 'Java', 'Go', 'C++', 'TypeScript', 'Ruby', 'PHP', 'C#', 'C', 'Rust', 'Kotlin',
    'Swift', 'Scala', 'Objective-C', 'Shell', 'Dart', 'R', 'Perl', 'Elixir', 'Haskell', 'Julia'
]

TOPIC_SAMPLES = ['machine-learning', 'web', 'cli', 'api', 'game', 'data', 'blockchain', 'test', 'image', 'video']

STAR_BUCKS = []
# Logarithmic star slices: tight at low, wider up top
for start in range(1, 1500, 20):
    STAR_BUCKS.append(f"stars:{start}..{start+19}")
for start in range(1500, 5000, 50):
    STAR_BUCKS.append(f"stars:{start}..{start+49}")
for start in range(5000, 30000, 250):
    STAR_BUCKS.append(f"stars:{start}..{start+249}")
for start in range(30000, 100000, 1000):
    STAR_BUCKS.append(f"stars:{start}..{start+999}")
STAR_BUCKS += [f"stars:>{v}" for v in (100000, 250000, 500000)]

# By language (mix star cutoff)
LANG_STAR = [f"language:{lang} stars:>10" for lang in SUPPORTED_LANGUAGES]
# By topic (mix star cutoff)
TOPIC_STAR = [f"topic:{tp} stars:>10" for tp in TOPIC_SAMPLES]
# By creation year
DATE_RANGES = [f"created:{year}-01-01..{year}-12-31 stars:>10" for year in range(2008, datetime.now().year+1)]

# Aggregate & shuffle
QUERY_POOL = STAR_BUCKS + LANG_STAR + TOPIC_STAR + DATE_RANGES
random.shuffle(QUERY_POOL)

# --- Worker logic: pop and crawl all pooled queries until DB >= 100K ---
async def diverse_crawl_worker(queue: Queue, pool, client, stop_event, worker_id: int):
    total_inserted = 0
    while not stop_event.is_set():
        try:
            bucket = await queue.get()
        except Exception:
            break
        if bucket is None:
            break
        query = bucket
        cursor = None
        fetched = 0
        seen_ids = set()
        while not stop_event.is_set():
            async with RATE_LOCK:
                await asyncio.sleep(RATE_DELAY + random.uniform(0.5, 1.5))
                data = await client.fetch_repos(query, cursor)
            if not data:
                break
            nodes = data.get("nodes", [])
            page_info = data.get("pageInfo", {})
            if nodes:
                # Dedup in-bucket, avoids rare bug
                new_nodes = [n for n in nodes if n["id"] not in seen_ids]
                for n in new_nodes:
                    seen_ids.add(n["id"])
                await insert_batch(pool, new_nodes)
                total_inserted += len(new_nodes)
                fetched += len(new_nodes)
                logging.info(f"[W{worker_id}] {query} got {fetched} repos")
            if not page_info.get("hasNextPage"):
                break
            cursor = page_info.get("endCursor")
        count = await get_repo_count(pool)
        if count >= TOTAL_TARGET or stop_event.is_set():
            stop_event.set()
            break
        queue.task_done()
    logging.info(f"[W{worker_id}] Finished diverse crawl, total inserted: {total_inserted}")

async def main():
    logging.info(f"Starting diverse async GitHub crawler (target={TOTAL_TARGET:,})")
    if not GITHUB_TOKEN:
        raise ValueError("Missing GITHUB_TOKEN environment variable")
    pool = await init_db_pool()
    async with aiohttp.ClientSession() as session:
        client = GitHubGraphQLClient(GITHUB_TOKEN, session)
        stop_event = asyncio.Event()
        bucket_queue = Queue()
        for q in QUERY_POOL:
            await bucket_queue.put(q)
        workers = [asyncio.create_task(diverse_crawl_worker(bucket_queue, pool, client, stop_event, i+1)) for i in range(WORKERS)]
        async def watcher():
            while not stop_event.is_set():
                count = await get_repo_count(pool)
                logging.info(f"DB count: {count:,}")
                if count >= TOTAL_TARGET:
                    stop_event.set()
                    for _ in range(WORKERS):
                        await bucket_queue.put(None)
                    break
                await asyncio.sleep(30)
        workers.append(asyncio.create_task(watcher()))
        await asyncio.gather(*workers)
    await pool.close()
    logging.info("Crawl complete — database has ≥100,000 repositories.")


if __name__ == "__main__":
    asyncio.run(main())
