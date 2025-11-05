import os
import requests
import psycopg2
import time
from datetime import datetime

# ===============================
# GitHub Crawler Configuration
# ===============================

GITHUB_API_URL = "https://api.github.com/graphql"
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_DB = os.getenv("POSTGRES_DB", "postgres")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")

# ===============================
# GraphQL Query (Fixed)
# ===============================
# Using an inline fragment `... on Repository` to avoid the union type error
query_template = """
query($cursor: String) {
  search(query: "stars:>100", type: REPOSITORY, first: 100, after: $cursor) {
    pageInfo {
      hasNextPage
      endCursor
    }
    nodes {
      ... on Repository {
        id
        name
        nameWithOwner
        stargazerCount
        primaryLanguage { name }
        createdAt
        updatedAt
        owner { login }
      }
    }
  }
}
"""

# ===============================
# Database Insertion Logic
# ===============================
def insert_repo(cursor, repo):
    cursor.execute("""
        INSERT INTO repositories (id, name, full_name, stars, language, created_at, updated_at, owner)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (id) DO UPDATE
        SET stars = EXCLUDED.stars,
            updated_at = EXCLUDED.updated_at;
    """, (
        repo["id"],
        repo["name"],
        repo["nameWithOwner"],
        repo["stargazerCount"],
        repo["primaryLanguage"]["name"] if repo["primaryLanguage"] else None,
        repo["createdAt"],
        repo["updatedAt"],
        repo["owner"]["login"]
    ))

# ===============================
# Main Crawler Logic
# ===============================
def crawl():
    headers = {"Authorization": f"Bearer {GITHUB_TOKEN}"}
    connection = psycopg2.connect(
        host=POSTGRES_HOST,
        database=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD
    )
    cursor = connection.cursor()

    total_repos = 0
    cursor_value = None

    print("🚀 Starting crawl of GitHub repositories...")

    while total_repos < 100000:
        try:
            variables = {"cursor": cursor_value}
            response = requests.post(GITHUB_API_URL, json={"query": query_template, "variables": variables}, headers=headers)

            # --- Handle HTTP errors ---
            if response.status_code != 200:
                print(f"❌ HTTP Error {response.status_code}: {response.text}")
                time.sleep(10)
                continue

            data = response.json()

            # --- Handle GraphQL errors ---
            if "errors" in data:
                print(f"⚠️ GraphQL errors: {data['errors']}")
                time.sleep(10)
                continue

            repos = data["data"]["search"]["nodes"]
            page_info = data["data"]["search"]["pageInfo"]

            for repo in repos:
                insert_repo(cursor, repo)
                total_repos += 1

            connection.commit()

            print(f"✅ Inserted {total_repos} repositories so far...")

            if not page_info["hasNextPage"]:
                break

            cursor_value = page_info["endCursor"]
            time.sleep(2)  # gentle rate limit pause

        except Exception as e:
            print(f"⚠️ Error: {e}")
            time.sleep(5)

    cursor.close()
    connection.close()
    print("🎉 Crawl completed successfully!")

# ===============================
# Run Script
# ===============================
if __name__ == "__main__":
    crawl()
