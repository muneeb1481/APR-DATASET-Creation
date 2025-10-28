"""
collect_apr_dataset_auto_date_fixed.py
-------------------------------------
Full Option C collector for RepairLLaMA-style APR dataset.

Features
• keyword rotation  • automatic date-window sliding
• multi-token rotation  • paging & checkpoint
• deduplication  • continuous CSV append
• timezone-safe datetime (no DeprecationWarning)
"""

import requests
import csv
import re
import time
import os
import json
import datetime
from tqdm import tqdm

# ---------------- CONFIG ----------------
GITHUB_TOKENS = [
  "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",  # 🔑 Token 1
    "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",  # 🔑 Token 2
    "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",  # 🔑 Token 3
]

OUTPUT_FILE = "python_repairllama_dataset.csv"
SEEN_FILE = "seen_commits.txt"
STATE_FILE = "collector_state.json"

MAX_COMMITS = 3000           # samples per run
FILES_LIMIT = 3              # skip commits touching > this many files
DAYS_PER_RANGE = 7           # each date window covers 7 days
MAX_PAGES_PER_QUERY = 10     # GitHub shows ≤ 1000 results per query
KEYWORDS = ["fix", "bug", "error", "issue", "exception", "crash", "typo"]

RATE_LIMIT_SLEEP = 3600      # wait 1 hour after all tokens hit limit
REQUEST_RETRY_SLEEP = 3
# ----------------------------------------

token_index = 0


# ---------------- UTILITIES ----------------
def save_state(state):
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f)


def load_state():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    today = datetime.datetime.now(datetime.UTC).date()
    state = {"keyword_index": 0, "date_end": today.strftime("%Y-%m-%d"), "page": 1}
    save_state(state)
    return state


def get_headers():
    return {
        "Authorization": f"token {GITHUB_TOKENS[token_index]}",
        "Accept": "application/vnd.github.cloak-preview+json"
    }


def switch_token():
    global token_index
    token_index = (token_index + 1) % len(GITHUB_TOKENS)
    print(f"\n🔄 Switched to GitHub token #{token_index + 1}\n")
    time.sleep(1)


def github_get(url):
    """Robust GET with token rotation."""
    global token_index
    attempts = 0
    while True:
        r = requests.get(url, headers=get_headers())
        if r.status_code == 200:
            return r.json()
        elif r.status_code == 403:
            print(f"⚠️ Token #{token_index+1} rate-limited.")
            prev = token_index
            switch_token()
            if token_index == 0 and prev == len(GITHUB_TOKENS) - 1:
                print("⏸️ All tokens exhausted — sleeping 1 hour for reset...")
                time.sleep(RATE_LIMIT_SLEEP)
        else:
            attempts += 1
            if attempts > 3:
                print(f"❌ HTTP {r.status_code} repeated; skipping.")
                return None
            time.sleep(REQUEST_RETRY_SLEEP)


# ---------------- PATCH / IR4 – OR2 ----------------
def parse_patch_hunks(patch):
    lines = patch.splitlines()
    hunks, i = [], 0
    header_re = re.compile(r"^@@ -(\d+)(?:,(\d+))? \+(\d+)(?:,(\d+))? @@")
    while i < len(lines):
        m = header_re.match(lines[i])
        if m:
            o_start = int(m.group(1))
            o_len = int(m.group(2)) if m.group(2) else 1
            n_start = int(m.group(3))
            n_len = int(m.group(4)) if m.group(4) else 1
            i += 1
            hunk_lines = []
            while i < len(lines) and not lines[i].startswith("@@ "):
                hunk_lines.append(lines[i])
                i += 1
            hunks.append(
                {"o_start": o_start, "o_len": o_len,
                 "n_start": n_start, "n_len": n_len, "lines": hunk_lines}
            )
        else:
            i += 1
    return hunks


def build_ir4_or2(buggy_code, fixed_code, patch):
    hunks = parse_patch_hunks(patch)
    if not hunks:
        return None, None
    buggy_lines_all = buggy_code.splitlines()
    o_starts = [h["o_start"] for h in hunks]
    o_ends = [h["o_start"] + h["o_len"] - 1 for h in hunks]
    start_idx = max(0, min(o_starts) - 1)
    end_idx = min(len(buggy_lines_all) - 1, max(o_ends) - 1)
    if start_idx > end_idx:
        return None, None

    buggy_snippet = buggy_lines_all[start_idx:end_idx + 1]
    fixed_snippet_lines = [
        ln[1:] for h in hunks for ln in h["lines"]
        if ln.startswith("+") and not ln.startswith("+++")
    ]
    if not fixed_snippet_lines:
        return None, None

    commented = "\n".join("# " + l if l.strip() else "#" for l in buggy_snippet)
    ir4 = "\n".join(
        buggy_lines_all[:start_idx] + [commented, "<FILL_ME>"] + buggy_lines_all[end_idx + 1:]
    )
    or2 = "\n".join(fixed_snippet_lines)
    return ir4, or2


# ---------------- GITHUB FETCH HELPERS ----------------
def search_commits_query(query, page=1):
    url = f"https://api.github.com/search/commits?q={requests.utils.requote_uri(query)}&sort=committer-date&order=desc&per_page=100&page={page}"
    return github_get(url)


def get_commit_details(repo, sha):
    url = f"https://api.github.com/repos/{repo}/commits/{sha}"
    return github_get(url)


def fetch_file(repo, sha, path):
    url = f"https://raw.githubusercontent.com/{repo}/{sha}/{path}"
    r = requests.get(url)
    return r.text if r.status_code == 200 else None


def advance_date(end_date_str, days):
    end_date = datetime.datetime.strptime(end_date_str, "%Y-%m-%d").date()
    new_end = end_date - datetime.timedelta(days=days)
    return new_end.strftime("%Y-%m-%d")


# ---------------- MAIN ----------------
def main():
    if not GITHUB_TOKENS:
        raise ValueError("Add at least one GitHub token.")

    state = load_state()
    seen_commits = set()
    if os.path.exists(SEEN_FILE):
        with open(SEEN_FILE, "r", encoding="utf-8") as f:
            seen_commits = {line.strip() for line in f if line.strip()}

    collected = 0
    page = state.get("page", 1)
    keyword_index = state.get("keyword_index", 0)
    date_end = state.get("date_end")

    file_exists = os.path.exists(OUTPUT_FILE)
    with open(OUTPUT_FILE, "a", encoding="utf-8", newline="") as csvfile:
        writer = csv.writer(csvfile)
        if not file_exists:
            writer.writerow([
                "repo", "file_path", "commit_sha", "commit_message",
                "input_representation", "output_representation",
                "buggy_code", "fixed_code"
            ])

        while collected < MAX_COMMITS:
            keyword = KEYWORDS[keyword_index % len(KEYWORDS)]
            end_date = datetime.datetime.strptime(date_end, "%Y-%m-%d").date()
            start_date = end_date - datetime.timedelta(days=DAYS_PER_RANGE - 1)
            date_range = f"{start_date.strftime('%Y-%m-%d')}..{end_date.strftime('%Y-%m-%d')}"
            query = f"{keyword}+language:Python+committer-date:{date_range}"

            print(f"\n🔎 Keyword: '{keyword}'  Date: {date_range}  Page: {page}  | Token #{token_index+1}")

            pages_used = 0
            while pages_used < MAX_PAGES_PER_QUERY and collected < MAX_COMMITS:
                res = search_commits_query(query, page)
                if not res or "items" not in res or not res["items"]:
                    print("ℹ️ No results for this page; advancing window/keyword.")
                    break

                for item in tqdm(res["items"], desc=f"Page {page}"):
                    if collected >= MAX_COMMITS:
                        break
                    repo = item["repository"]["full_name"]
                    sha = item["sha"]
                    if sha in seen_commits:
                        continue
                    commit = get_commit_details(repo, sha)
                    if not commit:
                        continue
                    parents = commit.get("parents", [])
                    if not parents:
                        continue
                    parent_sha = parents[0]["sha"]
                    files = commit.get("files", [])
                    if not files or len(files) > FILES_LIMIT:
                        continue

                    for f in files:
                        if not f["filename"].endswith(".py") or "patch" not in f:
                            continue
                        buggy = fetch_file(repo, parent_sha, f["filename"])
                        fixed = fetch_file(repo, sha, f["filename"])
                        if not buggy or not fixed or buggy.strip() == fixed.strip():
                            continue
                        ir4, or2 = build_ir4_or2(buggy, fixed, f["patch"])
                        if not ir4 or not or2:
                            continue
                        writer.writerow([
                            repo, f["filename"], sha,
                            commit["commit"]["message"].replace("\n", " "),
                            ir4, or2, buggy, fixed
                        ])
                        seen_commits.add(sha)
                        with open(SEEN_FILE, "a", encoding="utf-8") as sf:
                            sf.write(sha + "\n")
                        collected += 1
                        if collected % 100 == 0:
                            print(f"💾 Saved {collected}/{MAX_COMMITS} samples (total repos ≈ {len(set(item['repository']['full_name'] for item in res['items']))})")

                # save checkpoint
                page += 1
                pages_used += 1
                save_state({"keyword_index": keyword_index, "date_end": date_end, "page": page})
                time.sleep(1)

            # advance window / keyword
            date_end = advance_date(date_end, DAYS_PER_RANGE)
            page = 1
            keyword_index += 1
            save_state({"keyword_index": keyword_index, "date_end": date_end, "page": page})

    print(f"\n✅ Done — collected {collected} unique samples → {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
