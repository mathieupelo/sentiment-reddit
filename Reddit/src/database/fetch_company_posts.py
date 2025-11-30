"""
Fetches Reddit posts for companies using keywords from varrock.company_keywords table
and stores the top posts per day in the `tin.reddit_posts` table.

Usage:
    python -m Reddit.src.database.fetch_company_posts
    python -m Reddit.src.database.fetch_company_posts --date 2025-01-15
    python -m Reddit.src.database.fetch_company_posts --date 2025-01-15 --tickers RBLX TTWO
    python -m Reddit.src.database.fetch_company_posts --date 2025-01-15 --universe "GameCore-8"

Environment variables required:
    DATABASE_URL (main database for varrock schema)
    DATABASE_ORE_URL (ORE database for reddit_posts)
    REDDIT_CLIENT_ID
    REDDIT_CLIENT_SECRET
    REDDIT_USER_AGENT
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import time
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

import praw
import psycopg
from praw.models import Submission
from prawcore.exceptions import RequestException, ResponseException, ServerError, TooManyRequests

logger = logging.getLogger(__name__)

PER_DAY_CAP = 200
SEARCH_LIMIT = 100  # Reduced from 1000 to speed up processing
SLEEP_SECONDS = 0.3
SLEEP_EVERY = 20

# ORE database table is in 'tin' schema
REDDIT_POSTS_TABLE = "tin.reddit_posts"
PROCESSING_LOG_TABLE = "tin.reddit_processing_log"

# Processing log table creation SQL
PROCESSING_LOG_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS {PROCESSING_LOG_TABLE} (
    id BIGSERIAL PRIMARY KEY,
    ticker VARCHAR(20) NOT NULL,
    processed_date DATE NOT NULL,
    posts_found INTEGER DEFAULT 0,
    posts_inserted INTEGER DEFAULT 0,
    processing_started_at TIMESTAMPTZ NOT NULL,
    processing_completed_at TIMESTAMPTZ,
    status VARCHAR(20) DEFAULT 'completed',
    error_message TEXT,
    UNIQUE(ticker, processed_date)
);

CREATE INDEX IF NOT EXISTS idx_processing_log_ticker_date 
ON {PROCESSING_LOG_TABLE}(ticker, processed_date);

CREATE INDEX IF NOT EXISTS idx_processing_log_date 
ON {PROCESSING_LOG_TABLE}(processed_date);
"""

INSERT_SQL = f"""
insert into {REDDIT_POSTS_TABLE} (
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


def get_main_db_connection() -> psycopg.Connection:
    """Get connection to main database (for varrock schema)."""
    try:
        database_url = os.getenv('DATABASE_URL')
        if database_url:
            return psycopg.connect(database_url)
        
        host = os.getenv('DB_HOST')
        port = os.getenv('DB_PORT', '5432')
        user = os.getenv('DB_USER')
        password = os.getenv('DB_PASSWORD')
        database = os.getenv('DB_NAME')
        
        if not all([host, user, password, database]):
            raise MissingEnvironmentError(
                "Main database connection requires either DATABASE_URL or "
                "(DB_HOST, DB_USER, DB_PASSWORD, DB_NAME)"
            )
        
        return psycopg.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            dbname=database
        )
    except Exception as e:
        logger.error(f"Error connecting to main database: {e}")
        raise


def get_ore_db_connection() -> psycopg.Connection:
    """Get connection to ORE database (for reddit_posts)."""
    try:
        database_url = os.getenv('ORE_DATABASE_URL')
        if not database_url:
            database_url = os.getenv('DATABASE_ORE_URL')
        
        if database_url:
            return psycopg.connect(database_url)
        
        host = os.getenv('ORE_DB_HOST')
        port = os.getenv('ORE_DB_PORT', '5432')
        user = os.getenv('ORE_DB_USER')
        password = os.getenv('ORE_DB_PASSWORD')
        database = os.getenv('ORE_DB_NAME')
        
        if not all([host, user, password, database]):
            raise MissingEnvironmentError(
                "ORE database connection requires either ORE_DATABASE_URL, DATABASE_ORE_URL, or "
                "(ORE_DB_HOST, ORE_DB_USER, ORE_DB_PASSWORD, ORE_DB_NAME)"
            )
        
        return psycopg.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            dbname=database
        )
    except Exception as e:
        logger.error(f"Error connecting to ORE database: {e}")
        raise


def ensure_processing_log_table(ore_conn: psycopg.Connection) -> None:
    """Ensure the processing log table exists."""
    with ore_conn.cursor() as cur:
        # Create schema if it doesn't exist
        cur.execute("CREATE SCHEMA IF NOT EXISTS tin;")
        # Create table
        cur.execute(PROCESSING_LOG_TABLE_SQL)
    ore_conn.commit()


@dataclass
class KeywordInfo:
    """Represents a keyword with its priority."""
    keyword: str
    priority: int
    company_uid: str


@dataclass
class CompanyKeywords:
    """Represents a company with its keywords."""
    ticker: str
    company_name: str
    company_uid: str
    keywords: List[KeywordInfo]


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


def fetch_keywords_from_database(
    main_conn: psycopg.Connection,
    tickers: Optional[List[str]] = None,
    universe_name: Optional[str] = None,
) -> List[CompanyKeywords]:
    """
    Fetch keywords from varrock.company_keywords table.
    
    Args:
        main_conn: Connection to main database
        tickers: Optional list of tickers to filter by
        universe_name: Optional universe name to filter by (e.g., "GameCore-8")
    
    Returns:
        List of CompanyKeywords objects
    """
    query = """
        SELECT 
            t.ticker,
            COALESCE(ci.name, 'Unknown') as company_name,
            ck.company_uid,
            ck.keyword,
            COALESCE((ck.metadata->>'priority')::int, 999) as priority
        FROM varrock.company_keywords ck
        JOIN varrock.tickers t ON ck.company_uid = t.company_uid
        LEFT JOIN varrock.company_info ci ON ck.company_uid = ci.company_uid
    """
    
    conditions = []
    params = []
    
    if universe_name:
        query += """
            JOIN universe_companies uc ON ck.company_uid = uc.company_uid
            JOIN universes u ON uc.universe_id = u.id
        """
        conditions.append("u.name LIKE %s")
        params.append(f"%{universe_name}%")
    
    if tickers:
        placeholders = ",".join(["%s"] * len(tickers))
        conditions.append(f"t.ticker IN ({placeholders})")
        params.extend(tickers)
    
    if conditions:
        query += " WHERE " + " AND ".join(conditions)
    
    query += " ORDER BY t.ticker, priority ASC"
    
    with main_conn.cursor() as cur:
        cur.execute(query, params)
        rows = cur.fetchall()
    
    if not rows:
        logger.warning("No keywords found in database. Table may be empty.")
        return []
    
    # Group by ticker
    companies_dict: Dict[str, CompanyKeywords] = {}
    
    for row in rows:
        ticker, company_name, company_uid, keyword, priority = row
        
        if ticker not in companies_dict:
            companies_dict[ticker] = CompanyKeywords(
                ticker=ticker,
                company_name=company_name,
                company_uid=company_uid,
                keywords=[],
            )
        
        companies_dict[ticker].keywords.append(
            KeywordInfo(keyword=keyword, priority=priority, company_uid=company_uid)
        )
    
    # Sort keywords by priority for each company
    for company in companies_dict.values():
        company.keywords.sort(key=lambda k: k.priority)
    
    return list(companies_dict.values())


def is_date_ticker_processed(ore_conn: psycopg.Connection, target_date: date, ticker: str) -> bool:
    """
    Check if a date/ticker pair has already been processed.
    
    Uses the processing log table to track processed pairs, even when 0 posts were found.
    
    Args:
        ore_conn: Connection to ORE database
        target_date: Date to check
        ticker: Ticker symbol to check
    
    Returns:
        True if this date/ticker pair has been processed (regardless of post count), False otherwise
    """
    # Ensure we see the latest committed data
    # If autocommit is False, commit any pending transaction
    if not ore_conn.autocommit:
        try:
            ore_conn.commit()
        except Exception:
            # If commit fails (e.g., no transaction), that's okay
            pass
    
    with ore_conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT COUNT(*) 
            FROM {PROCESSING_LOG_TABLE}
            WHERE processed_date = %s 
              AND ticker = %s
              AND status = 'completed'
            """,
            (target_date, ticker),
            prepare=False,
        )
        count = cur.fetchone()[0]
        result = count > 0
        
        # Debug logging (only in verbose mode)
        if result:
            logger.debug(
                "Found existing processing log entry for %s on %s (count: %d)",
                ticker, target_date, count
            )
        
        return result


def start_processing_log(
    ore_conn: psycopg.Connection,
    ticker: str,
    target_date: date,
) -> None:
    """
    Create or update a processing log entry to mark processing as started.
    
    Args:
        ore_conn: Connection to ORE database
        ticker: Ticker symbol
        target_date: Date being processed
    """
    started_at = datetime.now(timezone.utc)
    try:
        with ore_conn.cursor() as cur:
            cur.execute(
                f"""
                INSERT INTO {PROCESSING_LOG_TABLE} 
                    (ticker, processed_date, processing_started_at, status)
                VALUES (%s, %s, %s, 'in_progress')
                ON CONFLICT (ticker, processed_date) 
                DO UPDATE SET 
                    processing_started_at = %s,
                    status = 'in_progress',
                    error_message = NULL
                """,
                (ticker, target_date, started_at, started_at),
                prepare=False,
            )
        if not ore_conn.autocommit:
            ore_conn.commit()
    except Exception as e:
        if not ore_conn.autocommit:
            ore_conn.rollback()
        raise


def complete_processing_log(
    ore_conn: psycopg.Connection,
    ticker: str,
    target_date: date,
    posts_found: int,
    posts_inserted: int,
    status: str = 'completed',
    error_message: Optional[str] = None,
) -> None:
    """
    Update processing log entry to mark processing as completed.
    
    Args:
        ore_conn: Connection to ORE database
        ticker: Ticker symbol
        target_date: Date that was processed
        posts_found: Number of posts found
        posts_inserted: Number of posts inserted
        status: Processing status ('completed', 'failed', 'skipped')
        error_message: Optional error message if processing failed
    """
    completed_at = datetime.now(timezone.utc)
    try:
        with ore_conn.cursor() as cur:
            cur.execute(
                f"""
                UPDATE {PROCESSING_LOG_TABLE}
                SET 
                    posts_found = %s,
                    posts_inserted = %s,
                    processing_completed_at = %s,
                    status = %s,
                    error_message = %s
                WHERE ticker = %s AND processed_date = %s
                """,
                (posts_found, posts_inserted, completed_at, status, error_message, ticker, target_date),
                prepare=False,
            )
        if not ore_conn.autocommit:
            ore_conn.commit()
    except Exception as e:
        if not ore_conn.autocommit:
            ore_conn.rollback()
        raise


def build_reddit_client() -> praw.Reddit:
    return praw.Reddit(
        client_id=require_env("REDDIT_CLIENT_ID"),
        client_secret=require_env("REDDIT_CLIENT_SECRET"),
        user_agent=require_env("REDDIT_USER_AGENT"),
    )


def fetch_submissions_for_keyword(
    reddit: praw.Reddit,
    keyword: str,
    target_date: date,
) -> List[Submission]:
    """
    Fetch Reddit submissions for a specific keyword on a target date.
    
    Args:
        reddit: PRAW Reddit client
        keyword: Keyword to search for
        target_date: Target date for posts
    
    Returns:
        List of Submission objects
    """
    # Calculate time range for the target date
    start_dt = datetime.combine(target_date, datetime.min.time()).replace(tzinfo=timezone.utc)
    end_dt = start_dt + timedelta(days=1)
    start_ts = start_dt.timestamp()
    end_ts = end_dt.timestamp()
    
    subreddit = reddit.subreddit("all")
    submissions: List[Submission] = []
    seen_ids: set[str] = set()
    request_count = 0
    
    # Search with different sort methods
    # Start with "top" as it's more likely to have relevant posts for the target date
    for sort in ("top", "new"):
        try:
            # Build query - use quotes for multi-word keywords
            query = f'"{keyword}"' if " " in keyword else keyword
            
            logger.debug("  Searching Reddit with sort='%s', query='%s'...", sort, query)
            results = subreddit.search(
                query,
                sort=sort,
                time_filter="day" if sort == "top" else "all",
                limit=SEARCH_LIMIT,
            )
            
            logger.debug("  Reddit search returned results iterator for '%s'", keyword)
            for submission in results:
                request_count += 1
                if request_count % SLEEP_EVERY == 0:
                    try:
                        time.sleep(SLEEP_SECONDS)
                    except KeyboardInterrupt:
                        # Re-raise KeyboardInterrupt so it can be handled at a higher level
                        logger.info("Processing interrupted by user")
                        raise
                
                # Log progress every 50 submissions
                if request_count % 50 == 0:
                    logger.debug("  Processed %d submissions for keyword '%s'...", request_count, keyword)
                
                if submission.id in seen_ids:
                    continue
                
                # Filter by target date
                if submission.created_utc < start_ts or submission.created_utc >= end_ts:
                    continue
                
                seen_ids.add(submission.id)
                submissions.append(submission)
                
                # Early exit if we have enough submissions for the target date
                # (we'll filter and rank later, but no need to fetch thousands if we only need 200)
                if len(submissions) >= PER_DAY_CAP * 2:
                    logger.debug("  Found enough submissions (%d), stopping fetch", len(submissions))
                    break
                
        except KeyboardInterrupt:
            # Re-raise KeyboardInterrupt immediately
            logger.info("Reddit API fetch interrupted by user")
            raise
        except TooManyRequests as exc:
            wait_seconds = getattr(exc, "sleep_seconds", 60)
            logger.warning("Rate limited for keyword '%s'; sleeping %.1f seconds", keyword, wait_seconds)
            time.sleep(wait_seconds)
        except (RequestException, ResponseException, ServerError) as exc:
            logger.warning("Transient error while fetching keyword '%s' (%s)", keyword, exc)
            time.sleep(5)
        except Exception as exc:
            logger.error("Unexpected error while fetching keyword '%s': %s", keyword, exc, exc_info=True)
    
    return submissions


def detect_keyword_match(text: str, keyword: str) -> bool:
    """Check if keyword matches in text (case-insensitive)."""
    haystack = text.lower()
    needle = keyword.lower()
    return needle in haystack


def filter_and_rank_submissions(
    submissions: Iterable[Submission],
    keyword: str,
    target_date: date,
    per_day_cap: int,
) -> List[Submission]:
    """
    Filter submissions by keyword match and rank by upvotes.
    
    Args:
        submissions: List of Reddit submissions
        keyword: Keyword that should match
        target_date: Target date
        per_day_cap: Maximum posts to return
    
    Returns:
        List of filtered and ranked submissions
    """
    matched: List[Submission] = []
    
    for submission in submissions:
        created_dt = datetime.fromtimestamp(submission.created_utc, tz=timezone.utc)
        if created_dt.date() != target_date:
            continue
        
        combined_text = f"{submission.title}\n{submission.selftext or ''}"
        if not detect_keyword_match(combined_text, keyword):
            continue
        
        matched.append(submission)
    
    # Sort by upvotes (score) descending
    matched.sort(key=lambda s: getattr(s, "score", 0), reverse=True)
    
    # Limit to per_day_cap
    return matched[:per_day_cap]


def submission_to_reddit_post(
    submission: Submission,
    keyword_matched: str,
    ticker: str,
) -> RedditPost:
    """Convert PRAW Submission to RedditPost dataclass."""
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
        ticker=ticker,
        keyword_matched=keyword_matched,
        collected_at=datetime.now(timezone.utc),
    )


def filter_existing_posts(ore_conn: psycopg.Connection, posts: List[RedditPost]) -> List[RedditPost]:
    """Filter out posts that already exist in database."""
    if not posts:
        return posts
    
    reddit_ids = [post.reddit_id for post in posts]
    with ore_conn.cursor() as cur:
        cur.execute(
            f"SELECT reddit_id FROM {REDDIT_POSTS_TABLE} WHERE reddit_id = ANY(%s)",
            (reddit_ids,),
            prepare=False,
        )
        existing = {row[0] for row in cur.fetchall()}
    
    if not existing:
        return posts
    
    filtered = [post for post in posts if post.reddit_id not in existing]
    logger.info("Skipped %d existing posts", len(posts) - len(filtered))
    return filtered


def insert_posts(ore_conn: psycopg.Connection, posts: List[RedditPost]) -> int:
    """Insert posts into database."""
    if not posts:
        return 0
    
    inserted = 0
    try:
        with ore_conn.cursor() as cur:
            for post in posts:
                # Use execute with prepare=False to avoid prepared statement conflicts
                cur.execute(INSERT_SQL, post.as_tuple(), prepare=False)
                cur.fetchone()
                inserted += 1
        if not ore_conn.autocommit:
            ore_conn.commit()
    except Exception as e:
        if not ore_conn.autocommit:
            ore_conn.rollback()
        raise
    return inserted


def process_company(
    reddit: praw.Reddit,
    main_conn: psycopg.Connection,
    ore_conn: psycopg.Connection,
    company: CompanyKeywords,
    target_date: date,
    per_day_cap: int,
    dry_run: bool,
) -> Tuple[int, int]:
    """
    Process a company: fetch posts using keywords in priority order until limit reached.
    
    Tracks processing in the log table, even when 0 posts are found.
    
    Args:
        reddit: PRAW Reddit client
        main_conn: Main database connection
        ore_conn: ORE database connection
        company: CompanyKeywords object
        target_date: Target date to fetch posts for
        per_day_cap: Maximum posts per day
        dry_run: If True, don't insert into database
    
    Returns:
        Tuple of (total_selected, total_inserted)
    """
    logger.info("\n%s (%s)", company.company_name, company.ticker)
    
    if not company.keywords:
        logger.warning("No keywords found for %s (%s)", company.company_name, company.ticker)
        # Log that we skipped due to no keywords
        if not dry_run:
            start_processing_log(ore_conn, company.ticker, target_date)
            complete_processing_log(
                ore_conn, company.ticker, target_date,
                posts_found=0, posts_inserted=0,
                status='skipped',
                error_message='No keywords found for company'
            )
        return 0, 0
    
    # Check if already processed
    if is_date_ticker_processed(ore_conn, target_date, company.ticker):
        logger.info("%s (%s) - Already processed, skipping", company.company_name, company.ticker)
        return 0, 0
    
    # Start processing log (even in dry-run, but we won't commit it)
    if not dry_run:
        start_processing_log(ore_conn, company.ticker, target_date)
    
    posts_found = 0
    posts_inserted = 0
    error_message = None
    status = 'completed'
    
    try:
        all_posts: List[RedditPost] = []
        posts_by_keyword: Dict[str, List[RedditPost]] = defaultdict(list)
        total_fetched = 0  # Track total submissions fetched before filtering
        
        # Process keywords in priority order (priority 1 = highest)
        for keyword_info in company.keywords:
            if len(all_posts) >= per_day_cap:
                logger.info("  → Reached daily cap (%d posts)", per_day_cap)
                break
            
            keyword = keyword_info.keyword
            
            # Fetch submissions for this keyword
            logger.info("  [%d] %s: Fetching from Reddit API...", keyword_info.priority, keyword)
            submissions = fetch_submissions_for_keyword(reddit, keyword, target_date)
            total_fetched += len(submissions)
            logger.debug("  [%d] %s: Fetched %d submissions from Reddit", keyword_info.priority, keyword, len(submissions))
            
            # Filter and rank
            filtered = filter_and_rank_submissions(submissions, keyword, target_date, per_day_cap)
            
            # Convert to RedditPost objects
            keyword_posts = [
                submission_to_reddit_post(sub, keyword, company.ticker)
                for sub in filtered
            ]
            
            # Filter out duplicates we've already collected
            existing_ids = {post.reddit_id for post in all_posts}
            keyword_posts = [post for post in keyword_posts if post.reddit_id not in existing_ids]
            
            # Add to collection (up to per_day_cap total)
            remaining_slots = per_day_cap - len(all_posts)
            keyword_posts = keyword_posts[:remaining_slots]
            
            all_posts.extend(keyword_posts)
            posts_by_keyword[keyword] = keyword_posts
            
            # Log keyword progress
            if len(keyword_posts) > 0:
                logger.info(
                    "  [%d] %s: +%d posts (total: %d/%d)",
                    keyword_info.priority,
                    keyword,
                    len(keyword_posts),
                    len(all_posts),
                    per_day_cap,
                )
            elif len(submissions) > 0:
                # Show that we fetched posts but they didn't match date/keyword
                logger.info(
                    "  [%d] %s: 0 posts (fetched %d, filtered by date/keyword)",
                    keyword_info.priority,
                    keyword,
                    len(submissions),
                )
        
        posts_found = len(all_posts)
        
        # Filter out posts that already exist in database
        all_posts = filter_existing_posts(ore_conn, all_posts)
        
        if dry_run:
            logger.info("  → Would insert %d new posts", len(all_posts))
            return len(all_posts), 0
        
        # Insert posts
        posts_inserted = insert_posts(ore_conn, all_posts)
        if posts_found > 0:
            logger.info("  → Inserted %d posts (%d found, %d already existed)", 
                       posts_inserted, posts_found, posts_found - posts_inserted)
        else:
            logger.info("  → No posts to insert (0 posts found for this date)")
        
    except KeyboardInterrupt:
        # Handle user interruption gracefully
        logger.info("\nProcessing interrupted by user for %s (%s)", company.company_name, company.ticker)
        error_message = "Processing interrupted by user"
        status = 'failed'
        # Don't re-raise, let it propagate naturally
        raise
    except Exception as exc:
        # Log the error
        error_message = str(exc)
        status = 'failed'
        logger.error(
            "Error processing %s (%s) for %s: %s",
            company.company_name,
            company.ticker,
            target_date,
            exc,
            exc_info=True,
        )
        # Re-raise to let caller handle if needed
        raise
    
    finally:
        # Always complete the log, even if there was an error or 0 posts
        if not dry_run:
            complete_processing_log(
                ore_conn,
                company.ticker,
                target_date,
                posts_found=posts_found,
                posts_inserted=posts_inserted,
                status=status,
                error_message=error_message,
            )
    
    return posts_found, posts_inserted


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fetch Reddit posts for companies using keywords from database."
    )
    parser.add_argument(
        "--date",
        type=str,
        help="Target date to fetch posts for (YYYY-MM-DD). Defaults to yesterday. Mutually exclusive with --start-date/--end-date.",
    )
    parser.add_argument(
        "--start-date",
        type=str,
        help="Start date for date range (YYYY-MM-DD). Use with --end-date for backfilling multiple dates.",
    )
    parser.add_argument(
        "--end-date",
        type=str,
        help="End date for date range (YYYY-MM-DD), inclusive. Use with --start-date for backfilling multiple dates.",
    )
    parser.add_argument(
        "--per-day-cap",
        type=int,
        default=PER_DAY_CAP,
        help="Maximum posts per company per calendar day to store.",
    )
    parser.add_argument(
        "--tickers",
        nargs="+",
        help="Subset of tickers to process.",
    )
    parser.add_argument(
        "--universe",
        type=str,
        help="Universe name to process (e.g., 'GameCore-8').",
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
    if verbose:
        level = logging.DEBUG
        format_str = "%(asctime)s %(levelname)s %(message)s"
    else:
        level = logging.INFO
        format_str = "%(message)s"  # Cleaner output without timestamps
    logging.basicConfig(level=level, format=format_str)


def parse_date_range(args: argparse.Namespace) -> List[date]:
    """
    Parse date arguments and return list of dates to process.
    
    Returns:
        List of date objects to process
    """
    dates: List[date] = []
    
    # Check for conflicting arguments
    if args.date and (args.start_date or args.end_date):
        logger.error("Cannot use --date with --start-date/--end-date. Use one or the other.")
        raise ValueError("Conflicting date arguments")
    
    if args.start_date and not args.end_date:
        logger.error("--start-date requires --end-date")
        raise ValueError("Missing --end-date")
    
    if args.end_date and not args.start_date:
        logger.error("--end-date requires --start-date")
        raise ValueError("Missing --start-date")
    
    # Parse single date
    if args.date:
        try:
            target_date = datetime.strptime(args.date, "%Y-%m-%d").date()
            dates.append(target_date)
        except ValueError:
            logger.error("Invalid date format. Use YYYY-MM-DD")
            raise
    
    # Parse date range
    elif args.start_date and args.end_date:
        try:
            start_date = datetime.strptime(args.start_date, "%Y-%m-%d").date()
            end_date = datetime.strptime(args.end_date, "%Y-%m-%d").date()
            
            if start_date > end_date:
                logger.error("Start date must be before or equal to end date")
                raise ValueError("Invalid date range")
            
            # Generate all dates in range (inclusive)
            current_date = start_date
            while current_date <= end_date:
                dates.append(current_date)
                current_date += timedelta(days=1)
            
        except ValueError as e:
            logger.error("Invalid date format. Use YYYY-MM-DD: %s", e)
            raise
    
    # Default to yesterday
    else:
        target_date = (datetime.now(timezone.utc) - timedelta(days=1)).date()
        dates.append(target_date)
    
    return dates


def main(argv: Optional[Sequence[str]] = None) -> int:
    try:
        args = parse_args(argv)
        configure_logging(args.verbose)
        
        # Parse date(s) to process
        try:
            dates_to_process = parse_date_range(args)
        except (ValueError, TypeError) as e:
            return 1
        
        if not dates_to_process:
            logger.error("No dates to process")
            return 1
        
        if len(dates_to_process) == 1:
            logger.info("Processing date: %s\n", dates_to_process[0])
        else:
            logger.info(
                "Processing date range: %s to %s (%d days)\n",
                dates_to_process[0],
                dates_to_process[-1],
                len(dates_to_process),
            )
        
        # Build Reddit client
        reddit = build_reddit_client()
        
        # Get database connections
        main_conn = get_main_db_connection()
        ore_conn = get_ore_db_connection()
        
        try:
            # Ensure processing log table exists
            ensure_processing_log_table(ore_conn)
            
            # Fetch companies and keywords from database (once for all dates)
            companies = fetch_keywords_from_database(
                main_conn,
                tickers=args.tickers,
                universe_name=args.universe,
            )
            
            if not companies:
                logger.warning("No companies with keywords found. Exiting.")
                return 1
            
            logger.info("Found %d companies with keywords\n", len(companies))
            
            per_day_cap = max(args.per_day_cap, 1)
            
            # Track totals across all dates
            total_selected = 0
            total_inserted = 0
            total_dates_processed = 0
            total_dates_skipped = 0
            
            # Process each date
            for target_date in dates_to_process:
                date_selected = 0
                date_inserted = 0
                date_processed = False
                
                # Process each company for this date
                for company in companies:
                    try:
                        selected_count, inserted_count = process_company(
                            reddit=reddit,
                            main_conn=main_conn,
                            ore_conn=ore_conn,
                            company=company,
                            target_date=target_date,
                            per_day_cap=per_day_cap,
                            dry_run=args.dry_run,
                        )
                        date_selected += selected_count
                        date_inserted += inserted_count
                        
                        if selected_count > 0 or inserted_count > 0:
                            date_processed = True
                    except KeyboardInterrupt:
                        logger.info("\nProcessing interrupted by user. Exiting gracefully.")
                        # Complete any in-progress logs before exiting
                        ore_conn.commit()
                        raise
                
                total_selected += date_selected
                total_inserted += date_inserted
                
                if date_processed:
                    total_dates_processed += 1
                    logger.info("\nDate %s: %d posts selected, %d inserted\n", 
                               target_date, date_selected, date_inserted)
                else:
                    total_dates_skipped += 1
                    logger.info("\nDate %s: No new posts (already processed or no matches)\n", target_date)
            
            # Final summary
            logger.info("\n" + "=" * 60)
            logger.info("SUMMARY")
            logger.info("=" * 60)
            logger.info("Date range: %s to %s (%d days)", 
                       dates_to_process[0], dates_to_process[-1], len(dates_to_process))
            logger.info("Dates processed: %d | Dates skipped: %d", total_dates_processed, total_dates_skipped)
            logger.info("Total posts: %d selected, %d inserted", total_selected, total_inserted)
            logger.info("=" * 60)
            
            return 0
            
        finally:
            main_conn.close()
            ore_conn.close()
    
    except MissingEnvironmentError as env_err:
        logger.error("Environment configuration error: %s", env_err)
        return 1
    except Exception as exc:
        logger.exception("Unexpected failure: %s", exc)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
