from datetime import datetime
from .graphql_client import GitHubClient
import psycopg2
import os

QUERY = """
query($cursor: String) {
  search(query: "stars:>0", type: REPOSITORY, first: 50, after: $cursor) {
    repositoryCount
    pageInfo {
      endCursor
      hasNextPage
    }
    nodes {
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
"""

def connect_db():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        dbname=os.getenv("POSTGRES_DB", "postgres"),
        user=os.getenv("POSTGRES_USER", "postgres"),
        password=os.getenv("POSTGRES_PASSWORD", "postgres"),
        port=int(os.getenv("POSTGRES_PORT", 5432))
    )

def upsert_repo(cur, repo):
    cur.execute("""
    INSERT INTO repositories (node_id, owner, name, full_name, stars, language, created_at, updated_at)
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
    ON CONFLICT (node_id)
    DO UPDATE SET stars=EXCLUDED.stars, updated_at=EXCLUDED.updated_at, last_crawled_at=now()
    """, (
        repo["id"],
        repo["owner"]["login"],
        repo["name"],
        repo["nameWithOwner"],
        repo["stargazerCount"],
        repo["primaryLanguage"]["name"] if repo["primaryLanguage"] else None,
        repo["createdAt"],
        repo["updatedAt"],
    ))

def crawl():
    client = GitHubClient()
    conn = connect_db()
    cur = conn.cursor()
    total = 0
    cursor = None

    while total < 100000:
        data = client.run_query(QUERY, {"cursor": cursor})
        repos = data["data"]["search"]["nodes"]
        for r in repos:
            upsert_repo(cur, r)
            total += 1
        conn.commit()
        info = data["data"]["search"]["pageInfo"]
        if not info["hasNextPage"]:
            break
        cursor = info["endCursor"]
        print(f"Fetched {total} repos so far...")

    cur.close()
    conn.close()
    print("✅ Done! Total:", total)
