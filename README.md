# Sentiment Reddit Supabase Connectivity

## Database Verification Script

- Ensure `DATABASE_ORE_URL` is exported in your shell (PowerShell: `setx DATABASE_ORE_URL "postgresql://..."` and restart the session; temporary session: `$Env:DATABASE_ORE_URL = "postgresql://..."`).
- Install dependencies: `pip install -r requirements.txt`.
- Run the verification script: `python -m Reddit.src.database.verify_connection`.
  - Add `--skip-insert` to only perform the connectivity check without the mock insert.

The script will attempt a `select 1`, create the `reddit_posts` table if it is missing, and perform a mock insert inside a rolled-back transaction to confirm write capability.

## Fetch and Store a Reddit Post

- Export Reddit API credentials (`REDDIT_CLIENT_ID`, `REDDIT_CLIENT_SECRET`, `REDDIT_USER_AGENT`) alongside `DATABASE_ORE_URL`.
- Run `python -m Reddit.src.database.fetch_single_post gaming --sort=top --ticker=TTWO --keywords="Take Two Interactive,Take-Two"` to fetch a single post and persist it.
  - Supported sorts: `hot` (default), `new`, `top` (defaults to past day).
  - `--ticker` (optional) stores the company ticker when a match is detected.
  - `--keywords` (optional) accepts a comma-separated list of phrases that qualify a post for storage; the matched phrase is saved into `keyword_matched`.
- The script saves the post into `reddit_posts` with the columns:
  `reddit_id, title, content, author, subreddit, created_datetime, upvotes, num_comments, url, ticker, keyword_matched, collected_at`.
- Inspect the printed output to verify the stored row or query the table directly via Supabase.

## Fetch Company Posts (7-Day Window)

- Companies and keywords are defined in `Reddit/src/database/fetch_company_posts.py` inside the `COMPANIES` dictionary.
- Run `python -m Reddit.src.database.fetch_company_posts` to collect posts for the last seven days.
  - Override settings with `--days`, `--per-day-cap`, and `--companies roblox take_two`.
  - Use `--dry-run` to inspect what would be inserted without writing to the database.
- Each company/day pair stores up to 200 of the highest-upvote posts whose title/body match the configured keywords or ticker (case-insensitive).
- Duplicate `reddit_id`s already present in `reddit_posts` are skipped automatically.


