-- =============================================================================
-- Gaming Analytics Pipeline - Staging Tables
-- =============================================================================
-- These tables hold raw data from extraction before transformation
-- =============================================================================

-- Drop existing tables if they exist
DROP TABLE IF EXISTS stg_reddit_comments CASCADE;
DROP TABLE IF EXISTS stg_reddit CASCADE;
DROP TABLE IF EXISTS stg_twitch CASCADE;
DROP TABLE IF EXISTS stg_rawg CASCADE;

-- =============================================================================
-- RAWG Staging Table
-- =============================================================================
CREATE TABLE stg_rawg (
    id SERIAL PRIMARY KEY,
    rawg_id INTEGER UNIQUE,
    name VARCHAR(255) NOT NULL,
    slug VARCHAR(255),
    released DATE,
    rating DECIMAL(3,2),
    rating_top INTEGER,
    ratings_count INTEGER,
    metacritic INTEGER,
    playtime INTEGER,
    updated TIMESTAMP,
    genres JSONB,
    tags JSONB,
    platforms JSONB,
    stores JSONB,
    developers JSONB,
    publishers JSONB,
    esrb_rating VARCHAR(50),
    background_image TEXT,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Index for faster lookups
CREATE INDEX idx_stg_rawg_name ON stg_rawg(name);
CREATE INDEX idx_stg_rawg_rawg_id ON stg_rawg(rawg_id);

-- =============================================================================
-- Twitch Staging Table
-- =============================================================================
CREATE TABLE stg_twitch (
    id SERIAL PRIMARY KEY,
    twitch_game_id VARCHAR(50),
    name VARCHAR(255) NOT NULL,
    box_art_url TEXT,
    igdb_id VARCHAR(50),
    viewer_count INTEGER,
    stream_count INTEGER,
    rank_position INTEGER,
    snapshot_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Index for faster lookups
CREATE INDEX idx_stg_twitch_name ON stg_twitch(name);
CREATE INDEX idx_stg_twitch_game_id ON stg_twitch(twitch_game_id);

-- =============================================================================
-- Reddit Posts Staging Table
-- =============================================================================
CREATE TABLE stg_reddit (
    id SERIAL PRIMARY KEY,
    post_id VARCHAR(20) UNIQUE,
    subreddit VARCHAR(50),
    title TEXT,
    selftext TEXT,
    score INTEGER,
    num_comments INTEGER,
    created_utc TIMESTAMP,
    url TEXT,
    author VARCHAR(100),
    data_source VARCHAR(50) DEFAULT 'kaggle',
    sentiment_score DECIMAL(4,3),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Index for faster lookups
CREATE INDEX idx_stg_reddit_post_id ON stg_reddit(post_id);
CREATE INDEX idx_stg_reddit_created ON stg_reddit(created_utc);

-- =============================================================================
-- Reddit Comments Staging Table (Pre-labeled sentiment from Kaggle)
-- =============================================================================
CREATE TABLE stg_reddit_comments (
    id SERIAL PRIMARY KEY,
    comment_text TEXT,
    sentiment VARCHAR(20),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Index for sentiment analysis
CREATE INDEX idx_stg_comments_sentiment ON stg_reddit_comments(sentiment);

-- =============================================================================
-- Verification
-- =============================================================================
-- Run this to verify tables were created
SELECT table_name 
FROM information_schema.tables 
WHERE table_schema = 'public' 
AND table_name LIKE 'stg_%';
