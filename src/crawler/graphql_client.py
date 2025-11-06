import asyncio
import logging
import aiohttp
from datetime import datetime, timezone

# ────────────────────────────────────────────────
# GitHub GraphQL Client (with rate-limit tracking)
# ────────────────────────────────────────────────
class GitHubGraphQLClient:
    """
    Optimized asynchronous client for GitHub GraphQL API.
    Includes rate-limit tracking and intelligent retry logic.
    """

    API_URL = "https://api.github.com/graphql"

    def __init__(self, token: str, session: aiohttp.ClientSession):
        if not token:
            raise ValueError("❌ Missing GitHub API token.")
        self.token = token
        self.session = session

    # ────────────────────────────────────────────────
    # Query with rate-limit info
    # ────────────────────────────────────────────────
    QUERY = """
    query ($queryString: String!, $cursor: String) {
      rateLimit {
        limit
        cost
        remaining
        resetAt
      }
      search(query: $queryString, type: REPOSITORY, first: 100, after: $cursor) {
        pageInfo {
          endCursor
          hasNextPage
        }
        nodes {
          ... on Repository {
            id
            name
            owner { login }
            stargazerCount
            forkCount
            createdAt
            updatedAt
            pushedAt
            url
            primaryLanguage { name }
          }
        }
      }
    }
    """

    # ────────────────────────────────────────────────
    # Fetch repositories
    # ────────────────────────────────────────────────
    async def fetch_repos(self, query_string: str, cursor: str = None):
        """
        Fetch one batch of repositories and log rate-limit status.
        Retries automatically on transient or rate-limit errors.
        """
        for attempt in range(6):
            try:
                async with self.session.post(
                    self.API_URL,
                    headers={"Authorization": f"Bearer {self.token}"},
                    json={
                        "query": self.QUERY,
                        "variables": {"queryString": query_string, "cursor": cursor},
                    },
                ) as resp:

                    # ─── Retry logic based on response status ───
                    if resp.status == 403:
                        logging.warning("⏳ Hit GitHub rate limit — sleeping 60s...")
                        await asyncio.sleep(60)
                        continue
                    elif resp.status >= 500:
                        logging.warning(f"⚠️ GitHub server error ({resp.status}) — retrying...")
                        await asyncio.sleep(5)
                        continue
                    elif resp.status != 200:
                        text = await resp.text()
                        logging.error(f"❌ Unexpected HTTP {resp.status}: {text}")
                        await asyncio.sleep(3)
                        continue

                    data = await resp.json()

                    if "errors" in data:
                        logging.warning(f"⚠️ GraphQL errors: {data['errors']}")
                        await asyncio.sleep(5)
                        continue

                    # ─── Extract and log rate-limit information ───
                    rl = data.get("data", {}).get("rateLimit")
                    if rl:
                        reset_time = datetime.strptime(
                            rl["resetAt"], "%Y-%m-%dT%H:%M:%SZ"
                        ).replace(tzinfo=timezone.utc)
                        minutes_left = (reset_time - datetime.now(timezone.utc)).total_seconds() / 60
                        logging.info(
                            f"⏱ Rate limit: {rl['remaining']}/{rl['limit']} left "
                            f"(cost={rl['cost']}) — resets in {minutes_left:.1f} min"
                        )

                        # Back off slightly if near exhaustion
                        if rl["remaining"] < 50:
                            logging.warning("🛑 Near rate-limit exhaustion — pausing 90s...")
                            await asyncio.sleep(90)

                    search_data = data.get("data", {}).get("search")
                    if not search_data:
                        logging.warning("⚠️ Empty or invalid search response — retrying...")
                        await asyncio.sleep(3)
                        continue

                    return {
                        "nodes": search_data.get("nodes", []),
                        "pageInfo": search_data.get("pageInfo", {}),
                    }

            except aiohttp.ClientError as e:
                logging.error(f"💥 Network error: {e}")
                await asyncio.sleep(3)
                continue
            except Exception as e:
                logging.exception(f"⚠️ Unexpected error: {e}")
                await asyncio.sleep(3)
                continue

        logging.error("❌ Failed after multiple retries for query: %s", query_string)
        return None
