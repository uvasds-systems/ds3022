"""
Find a recent commit on a repo's default branch that was created by merging
a pull request, then print the key values of that merge and its PR.

GraphQL version of 09_github_pr_vals.py. The REST version needs up to
1 + 2*N + 2 requests (commit list, /pulls per commit, PR details, commit
details). Here a single query walks the commit history AND pulls each
commit's associated PRs, stats, labels, and files in one round trip.

  repository -> defaultBranchRef -> target (Commit) -> history
    -> each commit: oid, parents, additions/deletions
       -> associatedPullRequests -> PR fields, labels, files

Note: pandas uses "squash and merge", so each PR lands as a single commit with
one parent. Matching PR.mergeCommit.oid to the commit oid works for squash,
rebase, and regular merges alike.

Rate limits are measured in points (5000/hr). `rateLimit` is added to the query
to show the cost.
https://docs.github.com/en/graphql/overview/rate-limits-and-node-limits-for-the-graphql-api
"""

import os
import time
import httpx

OWNER, NAME = "pandas-dev", "pandas"   # org/repo to inspect
MAX_COMMITS = 40                       # how many recent commits to scan for a PR merge
URL = "https://api.github.com/graphql"

# set your GH token before running
TOKEN = os.environ["GH_PAT"]
HEADERS = {"Authorization": f"Bearer {TOKEN}"}

# one query: recent commits on the default branch, each with its associated PRs
QUERY = """
query($owner: String!, $name: String!, $n: Int!) {
  rateLimit { cost remaining resetAt }
  repository(owner: $owner, name: $name) {
    defaultBranchRef {
      target {
        ... on Commit {
          history(first: $n) {
            nodes {
              oid
              additions
              deletions
              parents { totalCount }
              associatedPullRequests(first: 5) {
                nodes {
                  number title url createdAt mergedAt
                  author { login }
                  mergedBy { login }
                  headRefName baseRefName
                  mergeCommit { oid }
                  commits { totalCount }
                  changedFiles
                  labels(first: 20) { nodes { name } }
                  files(first: 100) { nodes { path additions deletions } }
                }
              }
            }
          }
        }
      }
    }
  }
}
"""

# POST the query; if quota is exhausted, sleep until reset and retry
def run_query(variables):
  while True:
    try:
      r = httpx.post(URL, headers=HEADERS, json={"query": QUERY, "variables": variables}, timeout=30)
      remaining = int(r.headers.get("x-ratelimit-remaining", 0))
      reset = int(r.headers.get("x-ratelimit-reset", time.time()))

      # GitHub returns 403/429 once the quota is exhausted
      if r.status_code in (403, 429) and remaining == 0:
        wait = max(reset - time.time(), 0) + 1
        print(f"Quota exhausted, sleeping {wait:.0f}s")
        time.sleep(wait)
        continue

      r.raise_for_status()
      data = r.json()

      # GraphQL returns HTTP 200 even for query errors -- check the "errors" key
      if "errors" in data:
        print(f"GraphQL errors: {data['errors']}")
        return None
      return data["data"]
    except httpx.HTTPError as e:
      print(f"POST {URL} failed: {e}")
      return None

# scan the commit history and return (commit, pr) for the first PR-merge commit
def find_pr_merge():
  data = run_query({"owner": OWNER, "name": NAME, "n": MAX_COMMITS})
  if not data:
    return None, None
  print(f"Query cost: {data['rateLimit']['cost']} point(s), {data['rateLimit']['remaining']} remaining")

  for c in data["repository"]["defaultBranchRef"]["target"]["history"]["nodes"]:
    # the merge commit is the one the PR points to as its mergeCommit
    for pr in c["associatedPullRequests"]["nodes"]:
      if pr["mergedAt"] and (pr["mergeCommit"] or {}).get("oid") == c["oid"]:
        return c, pr
  return None, None

# print the key values of the merge commit and its PR
def show_merge(commit, pr):
  login = lambda u: (u or {}).get("login")   # deleted users come back as null
  print(f"\nMerge commit for PR #{pr['number']} in {OWNER}/{NAME}")
  print(f"  Title:          {pr['title']}")
  print(f"  URL:            {pr['url']}")
  print(f"  Author:         {login(pr['author'])}")
  print(f"  Merged by:      {login(pr['mergedBy'])}")
  print(f"  Created at:     {pr['createdAt']}")
  print(f"  Merged at:      {pr['mergedAt']}")
  print(f"  Branch:         {pr['headRefName']} -> {pr['baseRefName']}")
  print(f"  Merge SHA:      {commit['oid']}")
  print(f"  Parents:        {commit['parents']['totalCount']}")
  print(f"  PR commits:     {pr['commits']['totalCount']}")
  print(f"  Labels:         {', '.join(l['name'] for l in pr['labels']['nodes']) or '-'}")
  print(f"  Files changed:  {pr['changedFiles']}")
  print(f"  Lines added:    +{commit['additions']}")
  print(f"  Lines deleted:  -{commit['deletions']}")
  print(f"  Lines changed:  {commit['additions'] + commit['deletions']}")

  # per-file breakdown (GraphQL exposes this on the PR, not the commit)
  print("\n  Per-file changes:")
  for f in pr["files"]["nodes"]:
    print(f"    {f['additions'] + f['deletions']:>6}  (+{f['additions']}/-{f['deletions']})  {f['path']}")


if __name__ == "__main__":
  commit, pr = find_pr_merge()
  if commit:
    show_merge(commit, pr)
  else:
    print(f"No PR merge found in last {MAX_COMMITS} commits of {OWNER}/{NAME}")
