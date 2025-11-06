import logging
import asyncio

GITHUB_API_URL = "https://api.github.com/graphql"

# GraphQL query
GITHUB_REPO_QUERY = """
query($search:String!, $cursor:String) {
  search(query:$search, type:REPOSITORY, first:100, after:$cursor) {
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
      }
    }
  }
}
"""

class GitHubGraphQLClient:
    """Minimal async GraphQL client for GitHub API v4."""

    def __init__(self, token: str, session):
        if not token:
            raise ValueError("Missing GITHUB_TOKEN environment variable.")
        self.token = token
        self.session = session

    async def fetch_repos(self, search: str, cursor: str | None = None) -> dict:
        """Perform one paginated GraphQL call to GitHub."""
        headers = {"Authorization": f"Bearer {self.token}"}

        for attempt in range(5):
            try:
                async with self.session.post(
                    GITHUB_API_URL,
                    json={"query": GITHUB_REPO_QUERY, "variables": {"search": search, "cursor": cursor}},
                    headers=headers,
                    timeout=60
                ) as r:

                    if r.status == 200:
                        data = await r.json()
                        if "errors" in data:
                            logging.warning(f"GraphQL error: {data['errors']}")
                            await asyncio.sleep(5)
                            continue
                        return data["data"]["search"]

                    elif r.status == 403:
                        reset = r.headers.get("X-RateLimit-Reset")
                        wait_for = max(60, int(reset) - int(asyncio.get_event_loop().time()) + 2) if reset else 60
                        logging.warning(f"Rate limit hit, sleeping {wait_for}s...")
                        await asyncio.sleep(wait_for)
                        continue

                    else:
                        text = await r.text()
                        logging.warning(f"GitHub API error {r.status}: {text[:150]}")
                        await asyncio.sleep(5)

            except asyncio.TimeoutError:
                logging.warning("Timeout while fetching repos, retrying...")
                await asyncio.sleep(3)
            except Exception as e:
                logging.warning(f"Network/JSON error: {e}, retrying...")
                await asyncio.sleep(3)

        raise RuntimeError("Failed to fetch after multiple retries.")
