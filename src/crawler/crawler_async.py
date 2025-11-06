import os
import asyncio
import logging
import hashlib
import aiohttp
import asyncpg
import time

# ==========================
# Configuration
# ==========================
GITHUB_API = "https://api.github.com/graphql"
TOTAL_TARGET = 100_000
CONCURRENT_WORKERS = 5  # number of parallel workers
TARGET_PER_WORKER = TOTAL_TARGET // CONCURRENT_WORKERS

STAR_QUERIES = [
    "stars:>10000",
    "stars:5000..9999",
    "stars:1000..4999",
    "stars:100..999",
    "stars:1..99"
]

# ✅ Fixed: Must use fragment "... on Repository"
QUERY = """
query($cursor:String, $search:String!) {
  search(query:$search, type:REPOSITORY, first:100, after:$cursor) {
    pageInfo {
      endCursor
      hasNextPage
    }
    nodes {
      ... on Repository {
        name
        owner { login }
        stargazerCount
        createdAt
        updatedAt
        url
        primaryLanguage { name }
      }
    }
  }
}
"""

# ==========================
# GitHub API Call with Rate-Limit Awareness
# ==========================
async def run_query(session, token, search, cursor=None):
    headers = {"Authorization": f"Bearer {token}"}

    for attempt in range(5):
        try:
            async with session.post(
                GITHUB_API,
                json={"query": QUERY, "variables": {"search": search, "cursor": cursor}},
                headers=headers,
                timeout=60
            ) as r:

                if r.status == 200:
                    # Inspect rate limit headers
                    remaining = r.headers.get("X-RateLimit-Remaining")
                    reset_at = r.headers.get("X-RateLimit-Reset")
                    if remaining is not None and int(remaining) <= 2:
                        wait_for = max(0, int(reset_at or 0) - int(time.time()) + 2)
                        logging.warning(f"⏳ Rate limit nearly exhausted. Sleeping {wait_for}s")
                        await asyncio.sleep(wait_for)

                    data = await r.json()
                    if "errors" in data:
                        logging.warning(f"⚠️ GraphQL errors: {data['errors']}")
                        await asyncio.sleep(10)
                        continue
                    return data

                elif r.status == 403:
                    text = await r.text()
                    logging.warning(f"⏳ Hit GitHub rate limit — {text[:120]}...")
                    # Parse reset time from headers
                    reset = r.headers.get("X-RateLimit-Reset")
                    if reset:
                        wait_for = max(0, int(reset) - int(time.time()) + 2)
                    else:
                        wait_for = 60
                    await asyncio.sleep(wait_for)
                    continue

                elif r.status in (502, 503):
                    logging.warning(f"🕐 GitHub server error {r.status}, retrying in 10s...")
                    await asyncio.sleep(10)
                    continue

                else:
                    text = await r.text()
                    logging.warning(f"⚠️ GitHub error {r.status}: {text[:200]}")
                    await asyncio.sleep(5)
                    continue

        except asyncio.TimeoutError:
            logging.warning("⚠️ Request timeout, retrying...")
            await asyncio.sleep(5)
        except aiohttp.ClientError as e:
            logging.warning(f"🌐 Network error: {e}, retrying...")
            await asyncio.sleep(5)

    raise RuntimeError("❌ Failed after multiple retries — API unavailable.")


# ==========================
# Database Insert (batch upsert)
# ==========================
async def insert_batch(pool, repos):
    if not repos:
        return

    async with pool.acquire() as conn:
        async with conn.transaction():
            for repo in repos:
                if not repo or "url" not in repo:
                    continue
                repo_id = hashlib.md5(repo["url"].encode()).hexdigest()
                await conn.execute("""
                    INSERT INTO repositories (id, name, full_name, stars, language, created_at, updated_at, owner)
                    VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
                    ON CONFLICT (id) DO UPDATE
                    SET stars = $4, updated_at = $7;
                """,
                repo_id,
                repo.get("name"),
                f"{repo['owner']['login']}/{repo['name']}" if repo.get("owner") else None,
                repo.get("stargazerCount", 0),
                repo["primaryLanguage"]["name"] if repo.get("primaryLanguage") else None,
                repo.get("createdAt"),
                repo.get("updatedAt"),
                repo["owner"]["login"] if repo.get("owner") else None
                )


# ==========================
# Worker Task
# ==========================
async def crawl_slice(pool, token, search, worker_id):
    async with aiohttp.ClientSession() as session:
        cursor = None
        total = 0
        logging.info(f"🧵 Worker {worker_id} starting query: {search}")

        while total < TARGET_PER_WORKER:
            data = await run_query(session, token, search, cursor)

            # Validate data
            if not data or "data" not in data or "search" not in data["data"]:
                logging.warning(f"⚠️ Invalid API response. Retrying after 10s...")
                await asyncio.sleep(10)
                continue

            search_data = data["data"]["search"]
            nodes = search_data.get("nodes", [])
            if not nodes:
                break

            await insert_batch(pool, nodes)
            total += len(nodes)

            info = search_data["pageInfo"]
            cursor = info.get("endCursor")

            logging.info(f"Worker {worker_id}: {total} repos crawled (cursor={cursor})")

            if not info.get("hasNextPage") or total >= TARGET_PER_WORKER:
                break

            await asyncio.sleep(0.5)  # gentle pacing

        logging.info(f"✅ Worker {worker_id} finished — {total} repos stored.")


# ==========================
# Main Entry
# ==========================
async def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(message)s")

    token = os.getenv("GITHUB_TOKEN")
    if not token:
        raise ValueError("❌ Missing GITHUB_TOKEN environment variable.")

    pool = await asyncpg.create_pool(
        user=os.getenv("POSTGRES_USER", "postgres"),
        password=os.getenv("POSTGRES_PASSWORD", "postgres"),
        database=os.getenv("POSTGRES_DB", "postgres"),
        host=os.getenv("POSTGRES_HOST", "127.0.0.1"),
        min_size=1, max_size=10,
    )

    tasks = []
    for i, search in enumerate(STAR_QUERIES):
        # stagger worker startup by 2 seconds to avoid simultaneous hits
        await asyncio.sleep(2)
        tasks.append(asyncio.create_task(crawl_slice(pool, token, search, i + 1)))

    await asyncio.gather(*tasks)
    await pool.close()

    logging.info("🎉 All workers completed successfully!")


if __name__ == "__main__":
    asyncio.run(main())
