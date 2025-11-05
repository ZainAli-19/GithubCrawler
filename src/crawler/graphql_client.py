import os
import requests
import time

class GitHubClient:
    def __init__(self, token=None):
        self.token = token or os.getenv("GITHUB_TOKEN")
        self.headers = {"Authorization": f"bearer {self.token}"}
        self.url = "https://api.github.com/graphql"

    def run_query(self, query, variables=None):
        """Send a GraphQL query and handle rate limits/retries."""
        for attempt in range(5):
            r = requests.post(self.url, json={"query": query, "variables": variables}, headers=self.headers)
            if r.status_code == 200:
                data = r.json()
                if "errors" in data:
                    print("GraphQL errors:", data["errors"])
                return data
            elif r.status_code == 403:
                print("Rate limited — sleeping...")
                time.sleep(60)
            else:
                print("Error:", r.status_code, r.text)
                time.sleep(5)
        raise Exception("Query failed after retries.")
