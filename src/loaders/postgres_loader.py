"""
PostgreSQL Loader
Loads extracted data from JSON staging files into PostgreSQL database.

Usage:
    from loaders.postgres_loader import PostgresLoader
    
    loader = PostgresLoader()
    loader.load_rawg_games("data/staging/rawg_games.json")
    loader.load_twitch_snapshots("data/staging/twitch_top_games.json")
    loader.load_reddit_posts("data/staging/reddit_posts.json")
"""

import os
import json
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime
from typing import Optional


class PostgresLoader:
    """
    Loads data into PostgreSQL staging tables with upsert logic.
    """
    
    def __init__(
        self,
        host: Optional[str] = None,
        port: Optional[str] = None,
        dbname: Optional[str] = None,
        user: Optional[str] = None,
        password: Optional[str] = None
    ):
        """
        Initialize the loader with database connection parameters.
        
        Args:
            host: Database host (default: from env or localhost)
            port: Database port (default: from env or 5432)
            dbname: Database name (default: from env or gaming_analytics)
            user: Database user (default: from env or analytics)
            password: Database password (default: from env or analytics123)
        """
        self.connection_params = {
            "host": host or os.environ.get("ANALYTICS_DB_HOST", "localhost"),
            "port": port or os.environ.get("ANALYTICS_DB_PORT", "5432"),
            "dbname": dbname or os.environ.get("ANALYTICS_DB_NAME", "gaming_analytics"),
            "user": user or os.environ.get("ANALYTICS_DB_USER", "analytics"),
            "password": password or os.environ.get("ANALYTICS_DB_PASSWORD", "analytics123")
        }
        
        self.conn = None
        self.cursor = None
    
    def connect(self):
        """Establish database connection."""
        if self.conn is None or self.conn.closed:
            print(f"Connecting to PostgreSQL at {self.connection_params['host']}:{self.connection_params['port']}...")
            self.conn = psycopg2.connect(**self.connection_params)
            self.cursor = self.conn.cursor()
            print("  ✓ Connected!")
    
    def disconnect(self):
        """Close database connection."""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
            print("  ✓ Disconnected from PostgreSQL")
    
    def __enter__(self):
        """Context manager entry."""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        if exc_type is None:
            self.conn.commit()
        else:
            self.conn.rollback()
        self.disconnect()
    
    # =========================================================================
    # RAWG Games Loader
    # =========================================================================
    def load_rawg_games(self, filepath: str) -> int:
        """
        Load RAWG games from JSON file into stg_rawg table.
        
        Args:
            filepath: Path to JSON file
            
        Returns:
            Number of rows loaded
        """
        print(f"\nLoading RAWG games from {filepath}...")
        
        with open(filepath, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        games = data.get('games', [])
        
        if not games:
            print("  No games found in file")
            return 0
        
        self.connect()
        
        # Prepare data for insertion
        rows = []
        for game in games:
            rows.append((
                game.get('rawg_id'),
                game.get('name'),
                game.get('slug'),
                game.get('released'),
                game.get('metacritic'),
                game.get('rating'),
                game.get('playtime'),
                game.get('genres'),  # Already JSON string
                game.get('tags'),    # Already JSON string
                game.get('platforms')  # Already JSON string
            ))
        
        # Upsert query - insert or update on conflict
        query = """
            INSERT INTO stg_rawg (rawg_id, name, slug, released, metacritic, rating, playtime, genres, tags, platforms)
            VALUES %s
            ON CONFLICT (rawg_id) DO UPDATE SET
                name = EXCLUDED.name,
                slug = EXCLUDED.slug,
                released = EXCLUDED.released,
                metacritic = EXCLUDED.metacritic,
                rating = EXCLUDED.rating,
                playtime = EXCLUDED.playtime,
                genres = EXCLUDED.genres,
                tags = EXCLUDED.tags,
                platforms = EXCLUDED.platforms,
                loaded_at = CURRENT_TIMESTAMP
        """
        
        # First, ensure rawg_id has a unique constraint
        try:
            self.cursor.execute("""
                ALTER TABLE stg_rawg ADD CONSTRAINT stg_rawg_rawg_id_unique UNIQUE (rawg_id)
            """)
            self.conn.commit()
        except psycopg2.errors.DuplicateTable:
            self.conn.rollback()  # Constraint already exists, that's fine
        
        # Execute batch insert
        execute_values(self.cursor, query, rows)
        self.conn.commit()
        
        print(f"  ✓ Loaded {len(rows)} games into stg_rawg")
        return len(rows)
    
    # =========================================================================
    # Twitch Snapshots Loader
    # =========================================================================
    def load_twitch_snapshots(self, filepath: str) -> int:
        """
        Load Twitch game snapshots from JSON file into stg_twitch table.
        
        Args:
            filepath: Path to JSON file
            
        Returns:
            Number of rows loaded
        """
        print(f"\nLoading Twitch snapshots from {filepath}...")
        
        with open(filepath, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        games = data.get('games', [])
        snapshot_time = data.get('snapshot_time', datetime.now().isoformat())
        
        if not games:
            print("  No games found in file")
            return 0
        
        self.connect()
        
        # Prepare data for insertion
        rows = []
        for game in games:
            rows.append((
                game.get('twitch_game_id'),
                game.get('name'),
                game.get('box_art_url'),
                game.get('viewer_count', 0),
                game.get('stream_count', 0),
                game.get('rank_position'),
                game.get('snapshot_time', snapshot_time)
            ))
        
        # Insert query (no upsert - we want multiple snapshots over time)
        query = """
            INSERT INTO stg_twitch (twitch_game_id, name, box_art_url, viewer_count, stream_count, rank_position, snapshot_time)
            VALUES %s
        """
        
        execute_values(self.cursor, query, rows)
        self.conn.commit()
        
        print(f"  ✓ Loaded {len(rows)} game snapshots into stg_twitch")
        return len(rows)
    
    # =========================================================================
    # Reddit Posts Loader
    # =========================================================================
    def load_reddit_posts(self, filepath: str) -> int:
        """
        Load Reddit posts from JSON file into stg_reddit table.
        
        Args:
            filepath: Path to JSON file
            
        Returns:
            Number of rows loaded
        """
        print(f"\nLoading Reddit posts from {filepath}...")
        
        with open(filepath, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        posts = data.get('data', [])
        
        if not posts:
            print("  No posts found in file")
            return 0
        
        self.connect()
        
        # Prepare data for insertion
        rows = []
        for post in posts:
            rows.append((
                post.get('post_id'),
                post.get('subreddit'),
                post.get('title'),
                post.get('selftext'),
                post.get('score', 0),
                post.get('num_comments', 0),
                post.get('created_utc'),
                post.get('author'),
                post.get('data_source')
            ))
        
        # First, ensure post_id has a unique constraint
        try:
            self.cursor.execute("""
                ALTER TABLE stg_reddit ADD CONSTRAINT stg_reddit_post_id_unique UNIQUE (post_id)
            """)
            self.conn.commit()
        except psycopg2.errors.DuplicateTable:
            self.conn.rollback()  # Constraint already exists
        
        # Upsert query
        query = """
            INSERT INTO stg_reddit (post_id, subreddit, title, selftext, score, num_comments, created_utc, author, data_source)
            VALUES %s
            ON CONFLICT (post_id) DO UPDATE SET
                score = EXCLUDED.score,
                num_comments = EXCLUDED.num_comments,
                loaded_at = CURRENT_TIMESTAMP
        """
        
        execute_values(self.cursor, query, rows)
        self.conn.commit()
        
        print(f"  ✓ Loaded {len(rows)} posts into stg_reddit")
        return len(rows)
    
    # =========================================================================
    # Reddit Comments Loader (for sentiment data)
    # =========================================================================
    def load_reddit_comments(self, filepath: str) -> int:
        """
        Load Reddit comments with sentiment from JSON file.
        We'll store these in a separate table for sentiment analysis.
        
        Args:
            filepath: Path to JSON file
            
        Returns:
            Number of rows loaded
        """
        print(f"\nLoading Reddit comments from {filepath}...")
        
        with open(filepath, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        comments = data.get('data', [])
        
        if not comments:
            print("  No comments found in file")
            return 0
        
        self.connect()
        
        # Create sentiment comments table if not exists
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS stg_reddit_comments (
                id SERIAL PRIMARY KEY,
                comment_id VARCHAR(50) UNIQUE,
                subreddit VARCHAR(50),
                text TEXT,
                sentiment_label VARCHAR(20),
                sentiment_score DECIMAL(4,3),
                data_source VARCHAR(50),
                loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        self.conn.commit()
        
        # Prepare data for insertion
        rows = []
        for comment in comments:
            rows.append((
                comment.get('comment_id'),
                comment.get('subreddit'),
                comment.get('text'),
                comment.get('sentiment_label'),
                comment.get('sentiment_score'),
                comment.get('data_source')
            ))
        
        # Upsert query
        query = """
            INSERT INTO stg_reddit_comments (comment_id, subreddit, text, sentiment_label, sentiment_score, data_source)
            VALUES %s
            ON CONFLICT (comment_id) DO UPDATE SET
                sentiment_label = EXCLUDED.sentiment_label,
                sentiment_score = EXCLUDED.sentiment_score,
                loaded_at = CURRENT_TIMESTAMP
        """
        
        execute_values(self.cursor, query, rows)
        self.conn.commit()
        
        print(f"  ✓ Loaded {len(rows)} comments into stg_reddit_comments")
        return len(rows)
    
    # =========================================================================
    # Utility Methods
    # =========================================================================
    def get_table_counts(self) -> dict:
        """
        Get row counts for all staging tables.
        
        Returns:
            Dictionary of table names and counts
        """
        self.connect()
        
        tables = ['stg_rawg', 'stg_twitch', 'stg_reddit', 'stg_reddit_comments']
        counts = {}
        
        for table in tables:
            try:
                self.cursor.execute(f"SELECT COUNT(*) FROM {table}")
                counts[table] = self.cursor.fetchone()[0]
            except psycopg2.errors.UndefinedTable:
                self.conn.rollback()
                counts[table] = 0
        
        return counts
    
    def clear_staging_tables(self):
        """Clear all staging tables (useful for fresh loads)."""
        self.connect()
        
        tables = ['stg_rawg', 'stg_twitch', 'stg_reddit', 'stg_reddit_comments']
        
        for table in tables:
            try:
                self.cursor.execute(f"TRUNCATE TABLE {table} RESTART IDENTITY")
                print(f"  ✓ Cleared {table}")
            except psycopg2.errors.UndefinedTable:
                self.conn.rollback()
        
        self.conn.commit()
    
    def verify_connection(self) -> bool:
        """
        Verify database connection is working.
        
        Returns:
            True if connection successful
        """
        try:
            self.connect()
            self.cursor.execute("SELECT 1")
            return True
        except Exception as e:
            print(f"Connection failed: {e}")
            return False


# =============================================================================
# Standalone execution for testing
# =============================================================================
if __name__ == "__main__":
    print("\n" + "=" * 50)
    print("  POSTGRESQL LOADER TEST")
    print("=" * 50)
    
    loader = PostgresLoader()
    
    # Test connection
    print("\n[1/5] Testing database connection...")
    if not loader.verify_connection():
        print("  ✗ Cannot connect to database. Make sure PostgreSQL is running:")
        print("    docker-compose start")
        exit(1)
    print("  ✓ Connection successful!")
    
    # Get current counts
    print("\n[2/5] Current table counts:")
    counts = loader.get_table_counts()
    for table, count in counts.items():
        print(f"  {table}: {count:,} rows")
    
    # Test loading RAWG data
    print("\n[3/5] Loading RAWG games...")
    rawg_file = "data/staging/rawg_games_test.json"
    if os.path.exists(rawg_file):
        loader.load_rawg_games(rawg_file)
    else:
        print(f"  - Skipped (file not found: {rawg_file})")
    
    # Test loading Twitch data
    print("\n[4/5] Loading Twitch snapshots...")
    twitch_file = "data/staging/twitch_top_games_test.json"
    if os.path.exists(twitch_file):
        loader.load_twitch_snapshots(twitch_file)
    else:
        print(f"  - Skipped (file not found: {twitch_file})")
    
    # Test loading Reddit data
    print("\n[5/5] Loading Reddit data...")
    reddit_posts_file = "data/staging/reddit_posts.json"
    reddit_comments_file = "data/staging/reddit_comments.json"
    
    if os.path.exists(reddit_posts_file):
        loader.load_reddit_posts(reddit_posts_file)
    else:
        print(f"  - Skipped posts (file not found: {reddit_posts_file})")
    
    if os.path.exists(reddit_comments_file):
        loader.load_reddit_comments(reddit_comments_file)
    else:
        print(f"  - Skipped comments (file not found: {reddit_comments_file})")
    
    # Final counts
    print("\n" + "-" * 50)
    print("Final table counts:")
    counts = loader.get_table_counts()
    for table, count in counts.items():
        print(f"  {table}: {count:,} rows")
    
    loader.disconnect()
    print("\n✓ PostgreSQL Loader test complete!")
