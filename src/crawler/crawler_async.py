import os
import asyncio
import logging
import hashlib
import aiohttp
import asyncpg

# ==========================
# Configuration
# ==========================
GITHUB_API = "https://api.github.com/graphql"
TOTAL_TARGET = 100_000
CONCURRENT_WORKERS = 5  # parallel workers
REPOS_PER_QUERY = 100
TARGET_PER_WORKER = TOTAL_TARGET // CONCURRENT_WORKERS

STAR_QUERIES = [
    "stars:>10000",
    "stars:5000..9999",
    "stars:1000..4999",
    "stars:100..999",
    "stars:1..99"
]

QUERY = """
query($cursor:String, $search:String!) {
  search(query:$search, type:REPOSITORY, first:100, after:$cursor) {
    pageInfo { endCursor hasNextPage }
    nodes {
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
"""

# ==========================
# GitHub API Call
# ==========================
async def run_query(session, token, search, cursor=None):
    for attempt in range(5):
        r = await session.post(
            GITHUB_API,
            json={"query": QUERY, "variables": {"search": search, "cursor": cursor}},
            headers={"Authorization": f"Bearer {token}"}
        )
        if r.status == 200:
            data = await r.json()
            if "errors" in data:
                logging.warning(f"GraphQL errors: {data['errors']}")
                await asyncio.sleep(15)
                continue  # retry outer loop
            return data
        elif r.status == 403:
            logging.warning("⏳ Hit GitHub rate limit — sleeping 60s...")
            await asyncio.sleep(60)
        else:
            text = await r.text()
            logging.warning(f"⚠️ GitHub error {r.status}: {text}")
            await asyncio.sleep(5)
    raise RuntimeError("❌ Failed after multiple retries")

# ==========================
# Database Insert
# ==========================
async def insert_batch(pool, repos):
    async with pool.acquire() as conn:
        async with conn.transaction():
            for repo in repos:
                if not repo or "url" not in repo:
                    continue
                uid = hashlib.md5(repo["url"].encode()).hexdigest()
                await conn.execute("""
                    INSERT INTO repositories (id, name, full_name, stars, language, created_at, updated_at, owner)
                    VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
                    ON CONFLICT (id) DO UPDATE
                    SET stars = $4, updated_at = $7;
                """,
                uid,
                repo["name"],
                f"{repo['owner']['login']}/{repo['name']}",
                repo["stargazerCount"],
                repo["primaryLanguage"]["name"] if repo["primaryLanguage"] else None,
                repo["createdAt"],
                repo["updatedAt"],
                repo["owner"]["login"])

# ==========================
# Worker Task
# ==========================
async def crawl_slice(pool, token, search, worker_id):
    async with aiohttp.ClientSession() as session:
        cursor = None
        total = 0
        logging.info(f"🧵 Worker {worker_id} starting slice: {search}")
        while True:
            data = await run_query(session, token, search, cursor)
            # --- Defensive error handling ---
            if not data or "data" not in data or "search" not in data["data"]:
                logging.warning(f"⚠️ Invalid or empty API response: {data}")
                await asyncio.sleep(10)  # back off briefly before retrying
                continue
            nodes = data["data"]["search"]["nodes"]
            if not nodes:
                break
            await insert_batch(pool, nodes)
            total += len(nodes)
            info = data["data"]["search"]["pageInfo"]
            cursor = info["endCursor"]
            logging.info(f"Worker {worker_id}: {total} repos crawled")
            if not info["hasNextPage"] or total >= TARGET_PER_WORKER:
                break
            await asyncio.sleep(0.5)
        logging.info(f"✅ Worker {worker_id} finished — {total} repos")

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
        await asyncio.sleep(2)  # stagger worker startup
        tasks.append(asyncio.create_task(crawl_slice(pool, token, search, i + 1)))
    await asyncio.gather(*tasks)
    await pool.close()
    logging.info("🎉 All workers completed successfully!")

if __name__ == "__main__":
    asyncio.run(main())
