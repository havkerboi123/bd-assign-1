#!/usr/bin/env python3
# Fetch last 10 posts from r/giki (once, no timer) with graph-ready fields.
# Writes append-only JSONL files: data/posts.jsonl, data/comments.jsonl, data/users.jsonl
# Also writes a one-time subreddit snapshot to data/subreddits.jsonl

import os
import json
from datetime import datetime, timezone
from pathlib import Path

import praw
from praw.models import MoreComments
from prawcore.exceptions import NotFound, Forbidden, ResponseException

# ---------- CONFIG ----------
SUBREDDIT_NAME = "giki"
POST_LIMIT = 10

DATA_DIR = Path("./data")
DATA_DIR.mkdir(exist_ok=True)

POSTS_FILE = DATA_DIR / "posts.jsonl"
COMMENTS_FILE = DATA_DIR / "comments.jsonl"
USERS_FILE = DATA_DIR / "users.jsonl"
SUBREDDITS_FILE = DATA_DIR / "subreddits.jsonl"

reddit = praw.Reddit(
    client_id="AfSEh3D7KDj1UC0p61cNRw",
    client_secret="WZ4mdemWIyYyKZqFflpZaXNj3ylZCw",
    user_agent="script:emir_scraper:1.0 (by u/ym2a_yt)",
    ratelimit_seconds=5,
)

# ---------- HELPERS ----------
def iso_day(ts_utc: float) -> str:
    return datetime.fromtimestamp(ts_utc, tz=timezone.utc).date().isoformat()

def write_jsonl(path: Path, record: dict):
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")

def safe_author(author):
    """
    Return (author_id, author_name) WITHOUT triggering a fetch.
    Accessing author.id causes a network call and 404 for deleted/suspended.
    """
    try:
        if author is None:
            return None, "[deleted]"
        # PRAW already sets .name on the Redditor object; this does not fetch.
        name = getattr(author, "name", None) or "[deleted]"
        # DO NOT access author.id (would fetch). Keep id None; we key on name fallback.
        return None, name
    except Exception:
        return None, "[deleted]"

def fetch_user_meta(name, _rid_unused):
    """
    Optional enrichment: total_karma + account_created.
    Guard against 404/403; return (None, None) on any failure.
    """
    if not name or name == "[deleted]":
        return None, None
    try:
        redditor = reddit.redditor(name)
        link_k = getattr(redditor, "link_karma", None)
        comment_k = getattr(redditor, "comment_karma", None)
        total_karma = (link_k or 0) + (comment_k or 0) if (link_k is not None or comment_k is not None) else None
        created_utc = float(getattr(redditor, "created_utc", 0.0)) or None
        return total_karma, created_utc
    except (NotFound, Forbidden, ResponseException):
        return None, None
    except Exception:
        return None, None

# ---------- SCRAPE ONCE ----------
def main():
    sr = reddit.subreddit(SUBREDDIT_NAME)

    # Subreddit snapshot (once per run)
    sub_meta = {
        "type": "subreddit",
        "name": SUBREDDIT_NAME,
        "description": getattr(sr, "public_description", None),
        "subscriber_count": getattr(sr, "subscribers", None),
        "created_utc": float(getattr(sr, "created_utc", 0.0)) or None,
        "fetched_at": datetime.now(timezone.utc).isoformat(),
    }
    write_jsonl(SUBREDDITS_FILE, sub_meta)

    seen_users = set()  # avoid duplicating user rows in this run

    for submission in sr.new(limit=POST_LIMIT):
        # ----- Post -----
        a_id, a_name = safe_author(submission.author)
        created = float(submission.created_utc)

        post_rec = {
            "type": "post",
            "id": submission.id,
            "subreddit": SUBREDDIT_NAME,
            "title": submission.title or "",
            "selftext": submission.selftext or "",
            "url": submission.url,
            "created_utc": created,
            "iso_day": iso_day(created),
            "score": int(submission.score) if submission.score is not None else None,
            "num_comments": int(submission.num_comments) if submission.num_comments is not None else 0,
            "author_id": a_id,          # stays None to avoid fetch; we still have name
            "author_name": a_name,
        }
        write_jsonl(POSTS_FILE, post_rec)

        # ----- User (post author) -----
        if a_name and a_name not in seen_users:
            total_karma, acct_created = fetch_user_meta(a_name, a_id)
            user_rec = {
                "type": "user",
                "id": a_id or f"name:{a_name}",   # fallback key based on name
                "name": a_name,
                "total_karma": total_karma,
                "account_created": acct_created,
            }
            write_jsonl(USERS_FILE, user_rec)
            seen_users.add(a_name)

        # ----- Comments (flattened) -----
        submission.comments.replace_more(limit=0)
        for c in submission.comments.list():
            if isinstance(c, MoreComments):
                continue

            ca_id, ca_name = safe_author(c.author)   # ← safe now
            c_created = float(c.created_utc)

            comment_rec = {
                "type": "comment",
                "id": c.id,
                "parent_id": c.parent_id.split("_", 1)[-1] if c.parent_id else None,
                "link_id": submission.id,  # the post id this comment belongs to
                "body": c.body,
                "created_utc": c_created,
                "iso_day": iso_day(c_created),
                "score": int(c.score) if c.score is not None else None,
                "depth": int(getattr(c, "depth", 0)),
                "is_submitter": bool(getattr(c, "is_submitter", False)),
                "author_id": ca_id,        # will be None for deleted/suspended
                "author_name": ca_name,
            }
            write_jsonl(COMMENTS_FILE, comment_rec)

            # User (comment author)
            if ca_name and ca_name not in seen_users and ca_name != "[deleted]":
                total_karma, acct_created = fetch_user_meta(ca_name, ca_id)
                user_rec = {
                    "type": "user",
                    "id": ca_id or f"name:{ca_name}",
                    "name": ca_name,
                    "total_karma": total_karma,
                    "account_created": acct_created,
                }
                write_jsonl(USERS_FILE, user_rec)
                seen_users.add(ca_name)

    print(f"Done. Wrote:\n- {POSTS_FILE}\n- {COMMENTS_FILE}\n- {USERS_FILE}\n- {SUBREDDITS_FILE}")

if __name__ == "__main__":
    main()
