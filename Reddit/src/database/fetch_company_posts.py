"""
Fetches Reddit posts for configured companies over a recent lookback window and
stores the top posts per day in the `reddit_posts` table.

Usage:
    python -m Reddit.src.database.fetch_company_posts
    python -m Reddit.src.database.fetch_company_posts --days 3 --per-day-cap 100
    python -m Reddit.src.database.fetch_company_posts --companies roblox take_two

Environment variables required:
    DATABASE_ORE_URL
    REDDIT_CLIENT_ID
    REDDIT_CLIENT_SECRET
    REDDIT_USER_AGENT
"""

from __future__ import annotations

import argparse
import logging
import os
import time
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

import praw
import psycopg
from praw.models import Submission
from prawcore.exceptions import RequestException, ResponseException, ServerError, TooManyRequests

logger = logging.getLogger(__name__)

DEFAULT_LOOKBACK_DAYS = 7
PER_DAY_CAP = 200
SEARCH_LIMIT = 1000
SLEEP_SECONDS = 0.3
SLEEP_EVERY = 20

POST_TABLE_SQL = """
create table if not exists reddit_posts (
    id bigint generated always as identity primary key,
    reddit_id text not null,
    title text not null,
    content text,
    author text,
    subreddit text not null,
    created_datetime timestamptz not null,
    upvotes integer default 0,
    num_comments integer default 0,
    url text not null,
    ticker text,
    keyword_matched text,
    collected_at timestamptz default now()
);
"""

ALTER_RENAME_TITLE_SQL = """
do $$
begin
    if exists (
        select 1 from information_schema.columns
        where table_name = 'reddit_posts' and column_name = 'post_title'
    ) then
        alter table reddit_posts rename column post_title to title;
    end if;
end;
$$;
"""

ALTER_RENAME_URL_SQL = """
do $$
begin
    if exists (
        select 1 from information_schema.columns
        where table_name = 'reddit_posts' and column_name = 'post_url'
    ) then
        alter table reddit_posts rename column post_url to url;
    end if;
end;
$$;
"""

ALTER_RENAME_CREATED_SQL = """
do $$
begin
    if exists (
        select 1 from information_schema.columns
        where table_name = 'reddit_posts' and column_name = 'created_utc'
    ) then
        alter table reddit_posts rename column created_utc to created_datetime;
    end if;
end;
$$;
"""

POST_TABLE_ALTERS = (
    "alter table reddit_posts add column if not exists reddit_id text",
    "alter table reddit_posts add column if not exists title text",
    "alter table reddit_posts alter column title set not null",
    "alter table reddit_posts add column if not exists content text",
    "alter table reddit_posts add column if not exists author text",
    "alter table reddit_posts add column if not exists subreddit text",
    "alter table reddit_posts alter column subreddit set not null",
    "alter table reddit_posts add column if not exists created_datetime timestamptz",
    "alter table reddit_posts alter column created_datetime set not null",
    "alter table reddit_posts add column if not exists upvotes integer default 0",
    "alter table reddit_posts alter column upvotes set default 0",
    "alter table reddit_posts add column if not exists num_comments integer default 0",
    "alter table reddit_posts alter column num_comments set default 0",
    "alter table reddit_posts add column if not exists url text",
    "alter table reddit_posts alter column url set not null",
    "alter table reddit_posts add column if not exists ticker text",
    "alter table reddit_posts add column if not exists keyword_matched text",
    "alter table reddit_posts add column if not exists collected_at timestamptz default now()",
    "alter table reddit_posts alter column collected_at set default now()",
)

INSERT_SQL = """
insert into reddit_posts (
    reddit_id,
    title,
    content,
    author,
    subreddit,
    created_datetime,
    upvotes,
    num_comments,
    url,
    ticker,
    keyword_matched,
    collected_at
)
values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
returning id, collected_at;
"""


class MissingEnvironmentError(RuntimeError):
    pass


def require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise MissingEnvironmentError(f"Environment variable {name} is required.")
    return value


def ensure_table(database_url: str) -> None:
    with psycopg.connect(database_url, autocommit=True) as conn:
        conn.execute(POST_TABLE_SQL)
        conn.execute(ALTER_RENAME_TITLE_SQL)
        conn.execute(ALTER_RENAME_URL_SQL)
        conn.execute(ALTER_RENAME_CREATED_SQL)
        for statement in POST_TABLE_ALTERS:
            conn.execute(statement)


@dataclass
class RedditPost:
    reddit_id: str
    title: str
    content: str
    author: Optional[str]
    subreddit: str
    created_datetime: datetime
    upvotes: int
    num_comments: int
    url: str
    ticker: Optional[str]
    keyword_matched: Optional[str]
    collected_at: datetime

    def as_tuple(self) -> tuple:
        return (
            self.reddit_id,
            self.title,
            self.content,
            self.author,
            self.subreddit,
            self.created_datetime,
            self.upvotes,
            self.num_comments,
            self.url,
            self.ticker,
            self.keyword_matched,
            self.collected_at,
        )


@dataclass(frozen=True)
class CompanyConfig:
    slug: str
    name: str
    ticker: str
    keywords: Sequence[str]

    @property
    def query(self) -> str:
        parts = [f'"{kw}"' if " " in kw else kw for kw in self.keywords]
        return "(" + " OR ".join(parts) + ")"


COMPANIES: Dict[str, CompanyConfig] = {
    "roblox": CompanyConfig(
        slug="roblox",
        name="Roblox Corporation",
        ticker="RBLX",
        keywords=("RBLX", "Roblox"),
    ),
    "netease": CompanyConfig(
        slug="netease",
        name="NetEase, Inc.",
        ticker="NTES",
        keywords=("NTES", "NetEase"),
    ),
    "take_two": CompanyConfig(
        slug="take_two",
        name="Take-Two Interactive Software, Inc.",
        ticker="TTWO",
        keywords=(
            "TTWO",
            "Take-Two Interactive",
            "Take Two Interactive",
        ),
    ),
}


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch Reddit posts for configured companies.")
    parser.add_argument("--days", type=int, default=DEFAULT_LOOKBACK_DAYS, help="Lookback window in days.")
    parser.add_argument(
        "--per-day-cap",
        type=int,
        default=PER_DAY_CAP,
        help="Maximum posts per company per calendar day to store.",
    )
    parser.add_argument(
        "--companies",
        nargs="+",
        choices=list(COMPANIES.keys()),
        help="Subset of company slugs to process.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Collect and display results without inserting into the database.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose logging.",
    )
    return parser.parse_args(argv)


def configure_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(level=level, format="%(asctime)s %(levelname)s %(message)s")


def build_reddit_client() -> praw.Reddit:
    return praw.Reddit(
        client_id=require_env("REDDIT_CLIENT_ID"),
        client_secret=require_env("REDDIT_CLIENT_SECRET"),
        user_agent=require_env("REDDIT_USER_AGENT"),
    )


def detect_keyword_match(text: str, config: CompanyConfig) -> Optional[str]:
    haystack = text.lower()

    if config.ticker.lower() in haystack:
        return config.ticker

    for keyword in config.keywords:
        if keyword.lower() in haystack:
            return keyword

    return None


def fetch_submissions_for_company(
    reddit: praw.Reddit,
    config: CompanyConfig,
    lookback_days: int,
) -> List[Submission]:
    start_time = datetime.now(timezone.utc) - timedelta(days=lookback_days)
    earliest_ts = start_time.timestamp()

    subreddit = reddit.subreddit("all")
    submissions: List[Submission] = []
    seen_ids: set[str] = set()
    request_count = 0

    for sort in ("new", "top"):
        try:
            results = subreddit.search(
                config.query,
                sort=sort,
                time_filter="week",
                limit=SEARCH_LIMIT,
            )
            for submission in results:
                request_count += 1
                if request_count % SLEEP_EVERY == 0:
                    time.sleep(SLEEP_SECONDS)

                if submission.id in seen_ids:
                    continue
                if submission.created_utc < earliest_ts:
                    continue

                seen_ids.add(submission.id)
                submissions.append(submission)
        except TooManyRequests as exc:
            wait_seconds = getattr(exc, "sleep_seconds", 60)
            logger.warning("Rate limited for %s; sleeping %.1f seconds", config.slug, wait_seconds)
            time.sleep(wait_seconds)
        except (RequestException, ResponseException, ServerError) as exc:
            logger.warning("Transient error while fetching %s (%s)", config.slug, exc)
            time.sleep(5)
        except Exception as exc:  # pragma: no cover - defensive
            logger.error("Unexpected error while fetching %s: %s", config.slug, exc, exc_info=True)

    return submissions


def filter_and_rank_submissions(
    submissions: Iterable[Submission],
    config: CompanyConfig,
    lookback_days: int,
    per_day_cap: int,
) -> List[Tuple[Submission, str]]:
    start_date = (datetime.now(timezone.utc) - timedelta(days=lookback_days)).date()

    grouped: Dict[datetime.date, List[Tuple[Submission, str]]] = defaultdict(list)
    total_considered = 0

    for submission in submissions:
        created_dt = datetime.fromtimestamp(submission.created_utc, tz=timezone.utc)
        if created_dt.date() < start_date:
            continue

        combined_text = f"{submission.title}\n{submission.selftext or ''}"
        keyword = detect_keyword_match(combined_text, config)
        if keyword is None:
            continue

        grouped[created_dt.date()].append((submission, keyword))
        total_considered += 1

    logger.info(
        "Company %s: %d submissions matched keywords across %d days",
        config.slug,
        total_considered,
        len(grouped),
    )

    selected: List[Tuple[Submission, str]] = []
    for day, entries in grouped.items():
        entries.sort(key=lambda item: getattr(item[0], "score", 0), reverse=True)
        limited = entries[:per_day_cap]
        logger.debug(
            "Company %s: selected %d/%d submissions for %s",
            config.slug,
            len(limited),
            len(entries),
            day,
        )
        selected.extend(limited)

    return selected


def submission_to_reddit_post(
    submission: Submission,
    keyword_matched: str,
    config: CompanyConfig,
) -> RedditPost:
    author_name = submission.author.name if submission.author else None
    created_datetime = datetime.fromtimestamp(submission.created_utc, tz=timezone.utc)

    return RedditPost(
        reddit_id=submission.id,
        title=submission.title,
        content=submission.selftext or "",
        author=author_name,
        subreddit=submission.subreddit.display_name,
        created_datetime=created_datetime,
        upvotes=getattr(submission, "score", 0),
        num_comments=getattr(submission, "num_comments", 0),
        url=submission.url,
        ticker=config.ticker,
        keyword_matched=keyword_matched,
        collected_at=datetime.now(timezone.utc),
    )


def filter_existing_posts(conn: psycopg.Connection, posts: List[RedditPost]) -> List[RedditPost]:
    if not posts:
        return posts

    reddit_ids = [post.reddit_id for post in posts]
    with conn.cursor() as cur:
        cur.execute(
            "select reddit_id from reddit_posts where reddit_id = any(%s)",
            (reddit_ids,),
        )
        existing = {row[0] for row in cur.fetchall()}

    if not existing:
        return posts

    filtered = [post for post in posts if post.reddit_id not in existing]
    logger.info("Skipped %d existing posts", len(posts) - len(filtered))
    return filtered


def insert_posts(conn: psycopg.Connection, posts: List[RedditPost]) -> int:
    if not posts:
        return 0

    inserted = 0
    with conn.cursor() as cur:
        for post in posts:
            cur.execute(INSERT_SQL, post.as_tuple())
            cur.fetchone()
            inserted += 1
    return inserted


def process_company(
    reddit: praw.Reddit,
    conn: psycopg.Connection,
    config: CompanyConfig,
    lookback_days: int,
    per_day_cap: int,
    dry_run: bool,
) -> Tuple[int, int]:
    submissions = fetch_submissions_for_company(reddit, config, lookback_days)
    selected = filter_and_rank_submissions(submissions, config, lookback_days, per_day_cap)
    posts = [
        submission_to_reddit_post(sub, keyword, config)
        for sub, keyword in selected
    ]
    posts = filter_existing_posts(conn, posts)

    if dry_run:
        logger.info(
            "Dry run: %s would insert %d new posts",
            config.slug,
            len(posts),
        )
        return len(selected), 0

    inserted = insert_posts(conn, posts)
    logger.info("Inserted %d posts for %s", inserted, config.slug)
    return len(selected), inserted


def main(argv: Optional[Sequence[str]] = None) -> int:
    try:
        args = parse_args(argv)
        configure_logging(args.verbose)

        reddit = build_reddit_client()
        database_url = require_env("DATABASE_ORE_URL")

        ensure_table(database_url)

        selected_slugs = args.companies or list(COMPANIES.keys())
        lookback_days = max(args.days, 1)
        per_day_cap = max(args.per_day_cap, 1)

        total_selected = 0
        total_inserted = 0

        with psycopg.connect(database_url, autocommit=True) as conn:
            for slug in selected_slugs:
                config = COMPANIES[slug]
                logger.info("Processing company %s (%s)", config.name, config.ticker)
                selected_count, inserted_count = process_company(
                    reddit=reddit,
                    conn=conn,
                    config=config,
                    lookback_days=lookback_days,
                    per_day_cap=per_day_cap,
                    dry_run=args.dry_run,
                )
                total_selected += selected_count
                total_inserted += inserted_count

        logger.info(
            "Completed run. Selected %d posts, inserted %d new rows.",
            total_selected,
            total_inserted,
        )
        return 0

    except MissingEnvironmentError as env_err:
        logger.error("Environment configuration error: %s", env_err)
        return 1
    except Exception as exc:  # pragma: no cover - defensive
        logger.exception("Unexpected failure: %s", exc)
        return 1


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())


