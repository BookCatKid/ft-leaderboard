import os
import json
import time
import random
import requests
from pathlib import Path

BASE_URL = "https://flavortown.hackclub.com/api/v1"
API_TOKEN = "AUTH TOKEN HERE"

HEADERS = {
    "Authorization": f"Bearer {API_TOKEN}"
}

PROJECTS_DIR = Path("projects")
PROJECTS_FILE = Path("data.json")
STATE_FILE = Path("state.json")

# Only fetch votes for projects that have ever been shipped.
# Skips "draft" (never submitted); keeps "rejected" since it may have had prior
# approved ship events with vote/payout history.
SHIPPED_STATUSES = {"submitted", "under_review", "approved", "rejected"}

PROJECTS_DIR.mkdir(parents=True, exist_ok=True)


# API rate limits (advertised by the server, two windows in parallel):
#   Sustained: 60 requests / 60 seconds
#   Burst:     20 requests / 5 seconds
# We track recent request timestamps in a deque and respect both windows.
from collections import deque

RATE_WINDOWS = [
    (60, 60.0),  # 60 reqs per 60s
    (20, 5.0),   # 20 reqs per 5s
]
SAFETY_MARGIN = 0.05  # extra sleep to avoid edge collisions

_recent_requests: deque[float] = deque()


def throttle():
    """Block until a new request can be made under all configured rate windows."""
    while True:
        now = time.time()
        # Drop timestamps outside the longest window we care about.
        cutoff = now - max(window for _, window in RATE_WINDOWS)
        while _recent_requests and _recent_requests[0] < cutoff:
            _recent_requests.popleft()

        wait = 0.0
        for limit, window in RATE_WINDOWS:
            if len(_recent_requests) < limit:
                continue
            # The (limit)-th most recent request must be older than `window`
            # before we can issue a new one.
            oldest_in_window = _recent_requests[-limit]
            wait_for_this = (oldest_in_window + window) - now
            if wait_for_this > wait:
                wait = wait_for_this

        if wait <= 0:
            _recent_requests.append(time.time())
            return
        time.sleep(wait + SAFETY_MARGIN)


def load_state():
    if STATE_FILE.exists():
        try:
            with open(STATE_FILE, "r", encoding="utf-8") as f:
                state = json.load(f)
            if isinstance(state, dict):
                return state
        except Exception:
            pass

    return {
        "seen_projects": [],
        "last_page": 1,
        "total_saved": 0,
    }


def save_state(state):
    tmp = STATE_FILE.with_suffix(".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2)
    tmp.replace(STATE_FILE)


def safe_folder_name(name: str) -> str:
    return "".join(c if c.isalnum() or c in " -_()" else "_" for c in name).strip()


def request_with_backoff(url, headers):
    delay = 1

    while True:
        throttle()
        r = requests.get(url, headers=headers)

        if r.status_code == 200:
            return r.json()

        if r.status_code == 429:
            sleep_time = delay + random.uniform(0, 0.8)
            print(f"429 hit → sleeping {sleep_time:.2f}s")
            time.sleep(sleep_time)
            delay *= 1.2
            continue

        r.raise_for_status()


def project_already_scraped(project):
    folder_name = safe_folder_name(project["title"])
    path = Path("projects") / folder_name / "votes.json"

    if not path.exists():
        return False

    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)

        # basic validation
        return "votes" in data and len(data["votes"]) > 0

    except Exception:
        return False


def load_project_list():
    if not PROJECTS_FILE.exists():
        return []

    try:
        with open(PROJECTS_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)

        if isinstance(data, dict):
            return data.get("projects", [])

        if isinstance(data, list):
            return data

    except Exception:
        return []

    return []


def save_project_list(projects):
    payload = {
        "projects": projects,
        "count": len(projects),
    }

    with open(PROJECTS_FILE, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)


def get_projects():
    state = load_state()
    page = state.get("last_page", 1)
    projects = load_project_list()
    seen_ids = {str(project["id"]) for project in projects if "id" in project}

    print(f"Resuming project scan from page {page}")

    while True:
        url = f"{BASE_URL}/projects?page={page}"
        data = request_with_backoff(url, HEADERS)

        page_projects = data.get("projects", [])
        if not page_projects:
            break

        print(f"Loaded page {page}: {len(page_projects)} projects")

        for project in page_projects:
            project_id = str(project.get("id"))
            if project_id in seen_ids:
                continue

            seen_ids.add(project_id)
            entry = dict(project)
            entry["id"] = project_id
            entry.setdefault("title", f"project_{project_id}")
            projects.append(entry)

        state["last_page"] = page
        save_state(state)
        save_project_list(projects)

        next_page = data.get("pagination", {}).get("next_page")
        if not next_page:
            break

        page = next_page

    return load_project_list()


def get_project_votes(project_id: str):
    url = f"{BASE_URL}/votes/records?project_id={project_id}&limit=1000"
    data = request_with_backoff(url, HEADERS)
    return data["votes"]


def get_project_results(project_id: str):
    url = f"{BASE_URL}/votes/results?project_id={project_id}"
    try:
        return request_with_backoff(url, HEADERS)
    except requests.HTTPError as e:
        if e.response is not None and e.response.status_code == 404:
            return None
        raise


def save_project(project, votes, results):
    folder_name = safe_folder_name(project["title"])
    path = Path("projects") / folder_name
    path.mkdir(parents=True, exist_ok=True)

    output = {
        "id": project["id"],
        "title": project["title"],
        "results": results,
        "votes": votes
    }

    with open(path / "votes.json", "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2)

    print(f"Saved {project['title']} ({len(votes)} votes)")


def main():
    if not API_TOKEN:
        raise ValueError("Missing API_TOKEN env var")

    state = load_state()
    seen_projects = set(state.get("seen_projects", []))

    projects = get_projects()
    print(f"Found {len(projects)} projects")
    print(f"Already seen: {len(seen_projects)}")

    dirty = False
    last_flush = time.time()
    FLUSH_EVERY_SECONDS = 5

    def flush_state(force=False):
        nonlocal dirty, last_flush
        now = time.time()
        if not dirty:
            return
        if not force and (now - last_flush) < FLUSH_EVERY_SECONDS:
            return
        state["seen_projects"] = list(seen_projects)
        save_state(state)
        dirty = False
        last_flush = now

    try:
        for project in projects:

            project_id = int(project["id"])

            ship_status = project.get("ship_status")
            if ship_status not in SHIPPED_STATUSES:
                if project_id not in seen_projects:
                    seen_projects.add(project_id)
                    dirty = True
                    flush_state()
                continue

            if project_id in seen_projects:
                continue

            # ✅ SKIP IF ALREADY SCRAPED
            if project_already_scraped(project):
                seen_projects.add(project_id)
                dirty = True
                flush_state()
                continue

            try:
                votes = get_project_votes(project["id"])
                results = get_project_results(project["id"])
                save_project(project, votes, results)

                seen_projects.add(project_id)
                state["total_saved"] = state.get("total_saved", 0) + 1
                dirty = True
                flush_state(force=True)

            except Exception as e:
                print(f"Failed {project['title']}: {e}")
    finally:
        flush_state(force=True)


if __name__ == "__main__":
    main()
