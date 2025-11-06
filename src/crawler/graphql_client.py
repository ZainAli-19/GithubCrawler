import asyncio
import logging
import aiohttp
from datetime import datetime, timezone

# ────────────────────────────────────────────────
# GitHub GraphQL Client (Enhanced 403 Handling)
# ────────────────────────────────────────────────
class GitHubGraphQLClient:
    """
    Optimized asynchronous client for GitHub GraphQL API.
    Includes robust handling for rate limits, abuse detection,
    and automatic backoff logic to avoid 403 slowdowns.
    """

    API_URL = "https://api.github.com/graphql"

    def __init__(self, token: str, session: aiohttp.ClientSession):
        if not token:
            raise ValueError("❌ Missing GitHub API token.")
        self.token = token
        self.session = session

    # ────────────────────────────────────────────────
    # GraphQL query
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
    # Fetch repositories (resilient retry + backoff)
    # ────────────────────────────────────────────────
    async def fetch_repos(self, query_string: str, cursor: str = None):
        """
        Fetch one batch of repositories.
        Handles:
          - Full rate-limit exhaustion
          - Secondary rate-limit (abuse detection)
          - Network and transient GraphQL errors
        """
        for attempt in range(10):
            try:
                async with self.session.post(
                    self.API_URL,
                    headers={
                        "Authorization": f"Bearer {self.token}",
                        "Accept": "application/vnd.github+json",
                        "X-Github-Api-Version": "2022-11-28",
                    },
                    json={
                        "query": self.QUERY,
                        "variables": {"queryString": query_string, "cursor": cursor},
                    },
                ) as resp:

                    # ────────────────────────────────────────────────
                    # Handle rate-limit and throttling responses
                    # ────────────────────────────────────────────────
                    if resp.status == 403:
                        text = await resp.text()

                        # 🧠 Detect "secondary rate limit" (abuse throttle)
                        if "secondary rate limit" in text.lower() or "abuse" in text.lower():
                            logging.warning("🚦 Secondary rate limit triggered — backing off 60s...")
                            await asyncio.sleep(60)
                            continue

                        # Attempt to parse JSON to check true rate-limit exhaustion
                        try:
                            data = await resp.json()
                        except Exception:
                            data = {}

                        rl = data.get("data", {}).get("rateLimit", {}) if data.get("data") else {}
                        remaining = rl.get("remaining")
                        reset_at = rl.get("resetAt")

                        if remaining == 0 and reset_at:
                            reset_dt = datetime.strptime(reset_at, "%Y-%m-%dT%H:%M:%SZ").replace(
                                tzinfo=timezone.utc
                            )
                            wait_sec = max((reset_dt - datetime.now(timezone.utc)).total_seconds(), 0)
                            logging.warning(
                                f"🕒 True rate limit reached — waiting {wait_sec/60:.1f} minutes until reset..."
                            )
                            await asyncio.sleep(wait_sec + 5)
                            continue

                        # Generic 403 with no details → short backoff
                        logging.warning("⚠️ Generic 403 error — sleeping 20s and retrying...")
                        await asyncio.sleep(20)
                        continue

                    elif resp.status >= 500:
                        logging.warning(f"⚠️ GitHub server error ({resp.status}) — retrying in 5s...")
                        await asyncio.sleep(5)
                        continue

                    elif resp.status != 200:
                        text = await resp.text()
                        logging.error(f"❌ Unexpected HTTP {resp.status}: {text}")
                        await asyncio.sleep(5)
                        continue

                    # ────────────────────────────────────────────────
                    # Parse valid response
                    # ────────────────────────────────────────────────
                    data = await resp.json()

                    if "errors" in data:
                        logging.warning(f"⚠️ GraphQL errors: {data['errors']}")
                        await asyncio.sleep(5)
                        continue

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
                        logging.warning("⚠️ Empty or invalid search response — retrying in 3s...")
                        await asyncio.sleep(3)
                        continue

                    return {
                        "nodes": search_data.get("nodes", []),
                        "pageInfo": search_data.get("pageInfo", {}),
                    }

            except aiohttp.ClientError as e:
                logging.error(f"💥 Network error: {e}")
                await asyncio.sleep(5)
                continue

            except Exception as e:
                logging.exception(f"⚠️ Unexpected error: {e}")
                await asyncio.sleep(5)
                continue

        logging.error("❌ Failed after multiple retries for query: %s", query_string)
        return None
