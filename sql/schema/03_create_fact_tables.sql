-- =============================================================================
-- Gaming Analytics Pipeline - Fact Tables
-- =============================================================================
-- Star schema fact tables for analytics
-- =============================================================================

-- Drop existing tables if they exist
DROP TABLE IF EXISTS agg_game_metrics CASCADE;
DROP TABLE IF EXISTS fact_reddit_posts CASCADE;
DROP TABLE IF EXISTS fact_twitch_snapshots CASCADE;

-- =============================================================================
-- Twitch Snapshots Fact Table
-- =============================================================================
-- Records point-in-time viewership data from Twitch
-- =============================================================================
CREATE TABLE fact_twitch_snapshots (
    snapshot_id SERIAL PRIMARY KEY,
    game_id INTEGER REFERENCES dim_games(game_id),
    twitch_game_id VARCHAR(50),
    twitch_game_name VARCHAR(255),
    snapshot_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    viewer_count INTEGER,
    stream_count INTEGER,
    rank_position INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for analytics queries
CREATE INDEX idx_fact_twitch_game_id ON fact_twitch_snapshots(game_id);
CREATE INDEX idx_fact_twitch_snapshot_time ON fact_twitch_snapshots(snapshot_time);
CREATE INDEX idx_fact_twitch_viewers ON fact_twitch_snapshots(viewer_count DESC);

-- =============================================================================
-- Reddit Posts Fact Table
-- =============================================================================
-- Stores Reddit posts with sentiment analysis
-- =============================================================================
CREATE TABLE fact_reddit_posts (
    post_id VARCHAR(20) PRIMARY KEY,
    subreddit VARCHAR(50),
    title TEXT,
    selftext TEXT,
    score INTEGER,
    num_comments INTEGER,
    created_utc TIMESTAMP,
    sentiment_score DECIMAL(4,3),
    mentioned_games TEXT[],
    data_source VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for analytics queries
CREATE INDEX idx_fact_reddit_sentiment ON fact_reddit_posts(sentiment_score);
CREATE INDEX idx_fact_reddit_created ON fact_reddit_posts(created_utc);
CREATE INDEX idx_fact_reddit_score ON fact_reddit_posts(score DESC);

-- =============================================================================
-- Aggregated Game Metrics Table
-- =============================================================================
-- Pre-computed metrics for dashboard performance
-- =============================================================================
CREATE TABLE agg_game_metrics (
    game_id INTEGER PRIMARY KEY REFERENCES dim_games(game_id),
    
    -- Twitch Metrics
    avg_twitch_viewers DECIMAL(12,2),
    max_twitch_viewers INTEGER,
    min_twitch_viewers INTEGER,
    twitch_snapshot_count INTEGER,
    
    -- Reddit Metrics
    reddit_mention_count INTEGER,
    avg_reddit_sentiment DECIMAL(4,3),
    total_reddit_engagement INTEGER,
    
    -- Computed Scores
    traction_score DECIMAL(5,2),
    popularity_rank INTEGER,
    
    -- Metadata
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Index for ranking queries
CREATE INDEX idx_agg_metrics_traction ON agg_game_metrics(traction_score DESC);
CREATE INDEX idx_agg_metrics_viewers ON agg_game_metrics(avg_twitch_viewers DESC);

-- =============================================================================
-- Trigger: Update timestamp on modification
-- =============================================================================
CREATE TRIGGER update_agg_metrics_modtime
    BEFORE UPDATE ON agg_game_metrics
    FOR EACH ROW
    EXECUTE FUNCTION update_modified_column();

-- =============================================================================
-- Verification
-- =============================================================================
SELECT table_name 
FROM information_schema.tables 
WHERE table_schema = 'public' 
AND (table_name LIKE 'fact_%' OR table_name LIKE 'agg_%');
