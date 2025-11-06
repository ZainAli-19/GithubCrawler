import asyncio
import logging
import aiohttp

# ────────────────────────────────────────────────
# GitHub GraphQL Client
# ────────────────────────────────────────────────
class GitHubGraphQLClient:
    """
    A lightweight asynchronous client for querying the GitHub GraphQL API.
    Handles retries, rate limits, and pagination.
    """

    API_URL = "https://api.github.com/graphql"

    def __init__(self, token: str, session: aiohttp.ClientSession):
        if not token:
            raise ValueError("❌ Missing GitHub API token.")
        self.token = token
        self.session = session

    # Correct query for Repository-only search (fixes union selection errors)
    QUERY = """
    query ($queryString: String!, $cursor: String) {
      search(query: $queryString, type: REPOSITORY, first: 100, after: $cursor) {
        pageInfo {
          endCursor
          hasNextPage
        }
        nodes {
          ... on Repository {
            id
            name
            owner {
              login
            }
            stargazerCount
            forkCount
            createdAt
            updatedAt
            pushedAt
            url
            primaryLanguage {
              name
            }
          }
        }
      }
    }
    """

    async def fetch_repos(self, query_string: str, cursor: str = None):
        """
        Fetch a batch of repositories for a given search query.
        Retries automatically on transient or rate-limit errors.
        """
        for attempt in range(5):
            try:
                async with self.session.post(
                    self.API_URL,
                    headers={"Authorization": f"Bearer {self.token}"},
                    json={
                        "query": self.QUERY,
                        "variables": {"queryString": query_string, "cursor": cursor},
                    },
                ) as resp:

                    # Retry on server or rate limit errors
                    if resp.status == 403:
                        logging.warning("⏳ Hit GitHub rate limit — sleeping 60s...")
                        await asyncio.sleep(60)
                        continue
                    elif resp.status >= 500:
                        logging.warning(f"⚠️ GitHub server error ({resp.status})")
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

                    search_data = data.get("data", {}).get("search")
                    if not search_data:
                        logging.warning("⚠️ Empty or invalid response from GitHub.")
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
