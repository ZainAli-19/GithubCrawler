import asyncio
import logging
import os
import sys
import asyncpg
from aiohttp import ClientSession, ClientError
from datetime import datetime

# --------------------------------------------------------------------
# ✅ Setup logging
# --------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)

# --------------------------------------------------------------------
# ✅ Environment variables
# --------------------------------------------------------------------
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "127.0.0.1")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")
POSTGRES_DB = os.getenv("POSTGRES_DB", "postgres")

MAX_RETRIES = 5
CONCURRENT_WORKERS = 10  # Adjust based on rate limits
GRAPHQL_URL = "https://api.github.com/graphql"

# --------------------------------------------------------------------
# ✅ Generate partitioned queries (stars + year)
# --------------------------------------------------------------------
STAR_BUCKETS = [
    "stars:>10000",
    "stars:5000..9999",
    "stars:1000..4999",
    "stars:500..999",
    "stars:200..499",
    "stars:100..199",
    "stars:50..99",
    "stars:20..49",
    "stars:10..19",
    "stars:5..9",
    "stars:2..4",
    "stars:1"
]

YEARS = [f"created:{year}-01-01..{year}-12-31" for year in range(2010, 2025)]

SEARCH_QUERIES = [
    f"{stars} {year} sort:stars-desc"
    for stars in STAR_BUCKETS
    for year in YEARS
]


# --------------------------------------------------------------------
# ✅ GraphQL query
# --------------------------------------------------------------------
GRAPHQL_QUERY = """
query ($queryString: String!, $cursor: String) {
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


# --------------------------------------------------------------------
# ✅ Fetch repositories for a single query slice
# --------------------------------------------------------------------
async def fetch_repositories(session, query_string, pool):
    cursor = None
    total_count = 0

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            while True:
                payload = {"query": GRAPHQL_QUERY, "variables": {"queryString": query_string, "cursor": cursor}}
                async with session.post(GRAPHQL_URL, json=payload) as resp:
                    if resp.status != 200:
                        text = await resp.text()
                        raise Exception(f"HTTP {resp.status}: {text}")

                    data = await resp.json()
                    repos = data.get("data", {}).get("search", {}).get("nodes", [])
                    page_info = data.get("data", {}).get("search", {}).get("pageInfo", {})

                    if not repos:
                        break

                    async with pool.acquire() as conn:
                        async with conn.transaction():
                            await conn.executemany("""
                                INSERT INTO repositories (
                                    id, name, owner, stars, forks, url, created_at, updated_at, language
                                )
                                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                                ON CONFLICT (id) DO UPDATE
                                SET stars = EXCLUDED.stars,
                                    forks = EXCLUDED.forks,
                                    updated_at = EXCLUDED.updated_at;
                            """, [
                                (
                                    r["id"],
                                    r["name"],
                                    r["owner"]["login"],
                                    r["stargazerCount"],
                                    r["forkCount"],
                                    r["url"],
                                    r["createdAt"],
                                    r["updatedAt"],
                                    (r["primaryLanguage"]["name"] if r["primaryLanguage"] else None)
                                )
                                for r in repos
                            ])

                    total_count += len(repos)
                    logging.info(f"✅ [{query_string}] inserted {len(repos)} (total: {total_count})")

                    if not page_info.get("hasNextPage"):
                        break
                    cursor = page_info["endCursor"]

            break  # success
        except Exception as e:
            logging.warning(f"⚠️ Query '{query_string}' attempt {attempt} failed: {e}")
            await asyncio.sleep(2 ** attempt)
    return total_count


# --------------------------------------------------------------------
# ✅ Worker pool runner
# --------------------------------------------------------------------
async def worker(name, queries, pool, session):
    total = 0
    for q in queries:
        count = await fetch_repositories(session, q, pool)
        total += count
    logging.info(f"🏁 Worker {name} done. Total: {total} repos.")


# --------------------------------------------------------------------
# ✅ Main async entrypoint
# --------------------------------------------------------------------
async def main():
    logging.info(f"🚀 Starting async GitHub crawler...")
    logging.info(f"💡 Total query partitions: {len(SEARCH_QUERIES)}")

    pool = await asyncpg.create_pool(
        host=POSTGRES_HOST,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        database=POSTGRES_DB,
        min_size=1,
        max_size=CONCURRENT_WORKERS,
    )

    headers = {"Authorization": f"Bearer {GITHUB_TOKEN}"}

    async with ClientSession(headers=headers) as session:
        # Split queries among workers evenly
        chunk_size = len(SEARCH_QUERIES) // CONCURRENT_WORKERS
        tasks = [
            asyncio.create_task(worker(i + 1, SEARCH_QUERIES[i * chunk_size:(i + 1) * chunk_size], pool, session))
            for i in range(CONCURRENT_WORKERS)
        ]
        await asyncio.gather(*tasks)

    await pool.close()
    logging.info("🎯 Crawling complete.")


if __name__ == "__main__":
    asyncio.run(main())
