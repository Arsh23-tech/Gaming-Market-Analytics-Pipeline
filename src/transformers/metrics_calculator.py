"""
Metrics Calculator
Computes aggregated metrics for games across all data sources.

This module:
1. Moves data from staging tables to fact tables
2. Calculates derived metrics (traction scores, engagement)
3. Populates the agg_game_metrics table

Usage:
    from transformers.metrics_calculator import MetricsCalculator
    
    calculator = MetricsCalculator()
    calculator.run_all_transformations()
"""

import os
import psycopg2
from typing import Optional
from datetime import datetime


class MetricsCalculator:
    """
    Calculates and aggregates metrics across all data sources.
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
        Initialize the calculator.
        
        Args:
            host, port, dbname, user, password: Database connection params
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
        
        print("MetricsCalculator initialized")
    
    def connect(self):
        """Establish database connection."""
        if self.conn is None or self.conn.closed:
            self.conn = psycopg2.connect(**self.connection_params)
            self.cursor = self.conn.cursor()
    
    def disconnect(self):
        """Close database connection."""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
    
    # =========================================================================
    # Staging to Dimension/Fact Tables
    # =========================================================================
    
    def populate_dim_games(self) -> int:
        """
        Populate dim_games from stg_rawg.
        Includes logic to determine is_indie and is_live_service flags.
        
        Returns:
            Number of rows affected
        """
        self.connect()
        
        print("\nPopulating dim_games from staging...")
        
        # First, simple insert/update from staging
        self.cursor.execute("""
            INSERT INTO dim_games (rawg_id, name, slug, release_date, metacritic_score, rating, playtime_hours)
            SELECT 
                rawg_id,
                name,
                slug,
                released::date,
                metacritic,
                rating,
                playtime
            FROM stg_rawg
            WHERE rawg_id IS NOT NULL
            ON CONFLICT (rawg_id) DO UPDATE SET
                name = EXCLUDED.name,
                slug = EXCLUDED.slug,
                release_date = EXCLUDED.release_date,
                metacritic_score = EXCLUDED.metacritic_score,
                rating = EXCLUDED.rating,
                playtime_hours = EXCLUDED.playtime_hours,
                updated_at = CURRENT_TIMESTAMP
        """)
        
        rows = self.cursor.rowcount
        self.conn.commit()
        
        print(f"  ✓ Inserted/updated {rows} games")
        
        # Now update is_indie and is_live_service flags based on tags
        self._update_game_flags()
        
        return rows
    
    def _update_game_flags(self):
        """Update is_indie and is_live_service flags based on tags."""
        print("  Updating game flags (is_indie, is_live_service)...")
        
        # Update is_indie flag
        # Games are considered indie if they have 'indie' tag
        self.cursor.execute("""
            UPDATE dim_games dg
            SET is_indie = TRUE
            FROM stg_rawg sr
            WHERE dg.rawg_id = sr.rawg_id
            AND sr.tags::text ILIKE '%indie%'
        """)
        indie_count = self.cursor.rowcount
        
        # Update is_live_service flag
        # Games are considered live-service if they have multiplayer/MMO/online tags
        self.cursor.execute("""
            UPDATE dim_games dg
            SET is_live_service = TRUE
            FROM stg_rawg sr
            WHERE dg.rawg_id = sr.rawg_id
            AND (
                sr.tags::text ILIKE '%multiplayer%'
                OR sr.tags::text ILIKE '%mmo%'
                OR sr.tags::text ILIKE '%online%'
                OR sr.tags::text ILIKE '%battle royale%'
                OR sr.tags::text ILIKE '%free to play%'
                OR sr.tags::text ILIKE '%live service%'
            )
        """)
        live_service_count = self.cursor.rowcount
        
        self.conn.commit()
        
        print(f"    Marked {indie_count} games as indie")
        print(f"    Marked {live_service_count} games as live-service")
    
    def populate_fact_twitch_snapshots(self) -> int:
        """
        Move Twitch data from staging to fact table.
        Links to dim_games where possible.
        
        Returns:
            Number of rows inserted
        """
        self.connect()
        
        print("\nPopulating fact_twitch_snapshots...")
        
        # Insert Twitch snapshots, trying to match with dim_games
        self.cursor.execute("""
            INSERT INTO fact_twitch_snapshots 
                (game_id, twitch_game_id, twitch_game_name, snapshot_time, viewer_count, stream_count, rank_position)
            SELECT 
                dg.game_id,
                st.twitch_game_id,
                st.name,
                st.snapshot_time,
                st.viewer_count,
                st.stream_count,
                st.rank_position
            FROM stg_twitch st
            LEFT JOIN dim_games dg ON dg.twitch_id = st.twitch_game_id
            WHERE st.snapshot_time IS NOT NULL
        """)
        
        rows = self.cursor.rowcount
        self.conn.commit()
        
        print(f"  ✓ Inserted {rows} Twitch snapshots")
        
        return rows
    
    def populate_fact_reddit_posts(self) -> int:
        """
        Move Reddit data from staging to fact table.
        
        Returns:
            Number of rows inserted
        """
        self.connect()
        
        print("\nPopulating fact_reddit_posts...")
        
        self.cursor.execute("""
            INSERT INTO fact_reddit_posts 
                (post_id, subreddit, title, selftext, score, num_comments, created_utc, sentiment_score, data_source)
            SELECT 
                post_id,
                subreddit,
                title,
                selftext,
                score,
                num_comments,
                created_utc::timestamp,
                sentiment_score,
                data_source
            FROM stg_reddit
            WHERE post_id IS NOT NULL
            ON CONFLICT (post_id) DO UPDATE SET
                score = EXCLUDED.score,
                num_comments = EXCLUDED.num_comments,
                sentiment_score = EXCLUDED.sentiment_score
        """)
        
        rows = self.cursor.rowcount
        self.conn.commit()
        
        print(f"  ✓ Inserted/updated {rows} Reddit posts")
        
        return rows
    
    # =========================================================================
    # Aggregated Metrics
    # =========================================================================
    
    def calculate_game_metrics(self) -> int:
        """
        Calculate aggregated metrics for each game and populate agg_game_metrics.
        
        Metrics calculated:
        - avg_twitch_viewers: Average viewers across all snapshots
        - max_twitch_viewers: Peak viewers observed
        - twitch_snapshot_count: Number of snapshots captured
        - reddit_mention_count: Number of Reddit posts (placeholder)
        - avg_reddit_sentiment: Average sentiment of mentions
        - total_reddit_engagement: Sum of scores + comments
        - traction_score: Composite score combining all metrics
        
        Returns:
            Number of games with metrics calculated
        """
        self.connect()
        
        print("\nCalculating game metrics...")
        
        # First, clear existing metrics
        self.cursor.execute("DELETE FROM agg_game_metrics")
        
        # Calculate metrics from Twitch data
        self.cursor.execute("""
            INSERT INTO agg_game_metrics (
                game_id,
                avg_twitch_viewers,
                max_twitch_viewers,
                twitch_snapshot_count,
                reddit_mention_count,
                avg_reddit_sentiment,
                total_reddit_engagement,
                traction_score,
                last_updated
            )
            SELECT 
                dg.game_id,
                COALESCE(AVG(fts.viewer_count), 0) as avg_twitch_viewers,
                COALESCE(MAX(fts.viewer_count), 0) as max_twitch_viewers,
                COUNT(fts.snapshot_id) as twitch_snapshot_count,
                0 as reddit_mention_count,  -- Placeholder, would need game mention extraction
                0 as avg_reddit_sentiment,   -- Placeholder
                0 as total_reddit_engagement, -- Placeholder
                -- Traction score formula (normalized composite)
                (
                    COALESCE(AVG(fts.viewer_count), 0) / 10000.0  -- Normalized viewer count
                    + COALESCE(dg.rating, 0) * 20                  -- Rating contribution (0-100)
                    + COALESCE(dg.metacritic_score, 0)             -- Metacritic (0-100)
                ) / 3 as traction_score,
                CURRENT_TIMESTAMP as last_updated
            FROM dim_games dg
            LEFT JOIN fact_twitch_snapshots fts ON dg.game_id = fts.game_id
            GROUP BY dg.game_id, dg.rating, dg.metacritic_score
        """)
        
        rows = self.cursor.rowcount
        self.conn.commit()
        
        print(f"  ✓ Calculated metrics for {rows} games")
        
        return rows
    
    def get_top_games_by_traction(self, limit: int = 20) -> list:
        """
        Get top games by traction score.
        
        Args:
            limit: Number of games to return
            
        Returns:
            List of tuples (game_name, traction_score, avg_viewers, rating)
        """
        self.connect()
        
        self.cursor.execute("""
            SELECT 
                dg.name,
                agm.traction_score,
                agm.avg_twitch_viewers,
                agm.max_twitch_viewers,
                dg.rating,
                dg.metacritic_score,
                dg.is_indie,
                dg.is_live_service
            FROM agg_game_metrics agm
            JOIN dim_games dg ON agm.game_id = dg.game_id
            ORDER BY agm.traction_score DESC
            LIMIT %s
        """, (limit,))
        
        return self.cursor.fetchall()
    
    def get_summary_stats(self) -> dict:
        """
        Get summary statistics across all tables.
        
        Returns:
            Dictionary with various counts and averages
        """
        self.connect()
        
        stats = {}
        
        # Game counts
        self.cursor.execute("SELECT COUNT(*) FROM dim_games")
        stats["total_games"] = self.cursor.fetchone()[0]
        
        self.cursor.execute("SELECT COUNT(*) FROM dim_games WHERE is_indie = TRUE")
        stats["indie_games"] = self.cursor.fetchone()[0]
        
        self.cursor.execute("SELECT COUNT(*) FROM dim_games WHERE is_live_service = TRUE")
        stats["live_service_games"] = self.cursor.fetchone()[0]
        
        # Twitch stats
        self.cursor.execute("SELECT COUNT(*), AVG(viewer_count), MAX(viewer_count) FROM fact_twitch_snapshots")
        row = self.cursor.fetchone()
        stats["twitch_snapshots"] = row[0]
        stats["avg_viewers"] = float(row[1]) if row[1] else 0
        stats["max_viewers"] = row[2] if row[2] else 0
        
        # Reddit stats
        self.cursor.execute("SELECT COUNT(*), AVG(score), AVG(sentiment_score) FROM fact_reddit_posts")
        row = self.cursor.fetchone()
        stats["reddit_posts"] = row[0]
        stats["avg_post_score"] = float(row[1]) if row[1] else 0
        stats["avg_sentiment"] = float(row[2]) if row[2] else 0
        
        return stats
    
    # =========================================================================
    # Full Pipeline
    # =========================================================================
    
    def run_all_transformations(self):
        """
        Run all transformations in the correct order.
        
        Order:
        1. Populate dim_games (with flags)
        2. Populate fact_twitch_snapshots
        3. Populate fact_reddit_posts
        4. Calculate aggregated metrics
        """
        print("\n" + "=" * 50)
        print("  RUNNING ALL TRANSFORMATIONS")
        print("=" * 50)
        
        start_time = datetime.now()
        
        # Step 1: Dimension tables
        self.populate_dim_games()
        
        # Step 2: Fact tables
        self.populate_fact_twitch_snapshots()
        self.populate_fact_reddit_posts()
        
        # Step 3: Aggregations
        self.calculate_game_metrics()
        
        # Summary
        elapsed = (datetime.now() - start_time).total_seconds()
        
        print("\n" + "-" * 50)
        print(f"Transformations complete in {elapsed:.1f} seconds")
        
        # Print summary stats
        stats = self.get_summary_stats()
        print(f"\nDatabase Summary:")
        print(f"  Games: {stats['total_games']} total ({stats['indie_games']} indie, {stats['live_service_games']} live-service)")
        print(f"  Twitch: {stats['twitch_snapshots']} snapshots, avg {stats['avg_viewers']:.0f} viewers")
        print(f"  Reddit: {stats['reddit_posts']} posts, avg sentiment {stats['avg_sentiment']:.3f}")


# =============================================================================
# Standalone execution for testing
# =============================================================================
if __name__ == "__main__":
    print("\n" + "=" * 50)
    print("  METRICS CALCULATOR TEST")
    print("=" * 50)
    
    calculator = MetricsCalculator()
    
    try:
        # Run all transformations
        calculator.run_all_transformations()
        
        # Show top games
        print("\n" + "=" * 50)
        print("  TOP GAMES BY TRACTION SCORE")
        print("=" * 50)
        
        top_games = calculator.get_top_games_by_traction(limit=10)
        
        if top_games:
            print(f"\n{'Rank':<5} {'Game':<35} {'Traction':<10} {'Viewers':<12} {'Rating':<8}")
            print("-" * 75)
            
            for i, game in enumerate(top_games, 1):
                name = game[0][:32] + "..." if len(game[0]) > 32 else game[0]
                traction = game[1] or 0
                viewers = game[2] or 0
                rating = game[4] or 0
                
                print(f"{i:<5} {name:<35} {traction:<10.2f} {viewers:<12.0f} {rating:<8.2f}")
        else:
            print("\n  No games found with metrics.")
        
        calculator.disconnect()
        print("\n✓ Metrics Calculator test complete!")
        
    except Exception as e:
        print(f"\n✗ Error: {e}")
        print("  Make sure PostgreSQL is running and has data loaded.")
