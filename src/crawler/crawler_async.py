import os
import asyncio
import logging
import asyncpg
from aiohttp import ClientSession
from datetime import datetime

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
GITHUB_GRAPHQL = "https://api.github.com/graphql"
TARGET_REPOS = 100_000
CONCURRENT_WORKERS = 10
MAX_RETRIES = 5
BATCH_SIZE = 100

# Star ranges and year slices — these partitions ensure wide coverage.
STAR_BUCKETS = [
    "stars:>20000",
    "stars:10000..19999",
    "stars:5000..9999",
    "stars:2000..4999",
    "stars:1000..1999",
    "stars:500..999",
    "stars:200..499",
    "stars:100..199",
    "stars:50..99",
    "stars:20..49",
    "stars:10..19",
    "stars:5..9",
    "stars:2..4",
    "stars:1",
]

YEARS = [f"created:{year}-01-01..{year}-12-31" for year in range(2010, 2025)]

SEARCH_QUERIES = [
    f"{stars} {year}"
    for stars in STAR_BUCKETS
    for year in YEARS
]

# ────────────────────────────────────────────────
# GraphQL query
# ────────────────────────────────────────────────
GITHUB_QUERY = """
query($queryString: String!, $cursor: String) {
  search(query: $queryString, type: REPOSITORY, first: 100, after: $cursor) {
    pageInfo {
      endCursor
      hasNextPage
    }
    nodes {
      id
      name
      owner { login }
      stargazerCount
      forkCount
      url
      createdAt
      updatedAt
      primaryLanguage { name }
    }
  }
}
"""

# ────────────────────────────────────────────────
# Utility: ISO date parser
# ────────────────────────────────────────────────
def parse_dt(ts):
    if not ts:
        return None
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except Exception:
        return None


# ────────────────────────────────────────────────
# Database helpers
# ────────────────────────────────────────────────
async def init_pool():
    return await asyncpg.create_pool(
        host=POSTGRES_HOST,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        database=POSTGRES_DB,
        min_size=1,
        max_size=CONCURRENT_WORKERS,
    )


async def get_repo_count(pool):
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT COUNT(*) AS c FROM repositories;")
        return row["c"] if row else 0


async def insert_repos(pool, repos):
    """Batch insert repositories into PostgreSQL."""
    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.executemany("""
                INSERT INTO repositories (
                    id, name, owner, stars, forks, url, created_at, updated_at, language
                )
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
                ON CONFLICT (id) DO UPDATE
                SET stars=$4, forks=$5, updated_at=$8;
            """, [
                (
                    r["id"],
                    r["name"],
                    r["owner"]["login"],
                    r["stargazerCount"],
                    r["forkCount"],
                    r["url"],
                    parse_dt(r["createdAt"]),
                    parse_dt(r["updatedAt"]),
                    (r["primaryLanguage"]["name"] if r["primaryLanguage"] else None)
                )
                for r in repos
            ])


# ────────────────────────────────────────────────
# GitHub fetch logic
# ────────────────────────────────────────────────
async def fetch_query(session, pool, query):
    cursor = None
    total_inserted = 0

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            while True:
                async with session.post(
                    GITHUB_GRAPHQL,
                    json={"query": GITHUB_QUERY, "variables": {"queryString": query, "cursor": cursor}},
                ) as r:
                    if r.status != 200:
                        txt = await r.text()
                        logging.warning(f"⚠️ HTTP {r.status}: {txt[:200]}")
                        await asyncio.sleep(10)
                        continue

                    data = await r.json()
                    if "errors" in data:
                        logging.warning(f"⚠️ GraphQL errors: {data['errors']}")
                        await asyncio.sleep(5)
                        continue

                    nodes = data["data"]["search"]["nodes"]
                    if not nodes:
                        break

                    await insert_repos(pool, nodes)
                    total_inserted += len(nodes)
                    logging.info(f"📦 {query[:40]}... → inserted {len(nodes)} (total {total_inserted})")

                    info = data["data"]["search"]["pageInfo"]
                    if not info.get("hasNextPage"):
                        break
                    cursor = info.get("endCursor")

                    # Short delay to avoid abuse detection
                    await asyncio.sleep(0.3)

            break  # success
        except Exception as e:
            logging.warning(f"Retry {attempt} for query '{query[:40]}...': {e}")
            await asyncio.sleep(2 ** attempt)

    return total_inserted


# ────────────────────────────────────────────────
# Worker: process batch of queries
# ────────────────────────────────────────────────
async def worker(name, pool, session, queries):
    inserted = 0
    for q in queries:
        # Check stop condition before every query
        total = await get_repo_count(pool)
        if total >= TARGET_REPOS:
            logging.info(f"🏁 Worker {name}: stopping (DB reached {total} repos)")
            return

        inserted += await fetch_query(session, pool, q)
    logging.info(f"✅ Worker {name} finished with {inserted} inserts.")


# ────────────────────────────────────────────────
# Main entry
# ────────────────────────────────────────────────
async def main():
    if not GITHUB_TOKEN:
        raise ValueError("❌ Missing GITHUB_TOKEN environment variable")

    logging.info("🚀 Starting GitHub repo crawler")
    logging.info(f"💡 Total partitions: {len(SEARCH_QUERIES)}")

    pool = await init_pool()
    headers = {"Authorization": f"Bearer {GITHUB_TOKEN}"}

    async with ClientSession(headers=headers) as session:
        # Split the search queries evenly across workers
        chunk_size = len(SEARCH_QUERIES) // CONCURRENT_WORKERS
        query_slices = [
            SEARCH_QUERIES[i * chunk_size:(i + 1) * chunk_size]
            for i in range(CONCURRENT_WORKERS)
        ]

        tasks = [
            asyncio.create_task(worker(i + 1, pool, session, query_slices[i]))
            for i in range(CONCURRENT_WORKERS)
        ]

        await asyncio.gather(*tasks)

    final_count = await get_repo_count(pool)
    await pool.close()
    logging.info(f"🎯 Done! Total repositories in DB: {final_count}")


if __name__ == "__main__":
    asyncio.run(main())
