-- =============================================================================
-- Gaming Analytics Pipeline - Dimension Tables
-- =============================================================================
-- Star schema dimension tables for analytics
-- =============================================================================

-- Drop existing tables if they exist
DROP TABLE IF EXISTS bridge_game_genres CASCADE;
DROP TABLE IF EXISTS dim_genres CASCADE;
DROP TABLE IF EXISTS dim_games CASCADE;

-- =============================================================================
-- Games Dimension Table
-- =============================================================================
CREATE TABLE dim_games (
    game_id SERIAL PRIMARY KEY,
    rawg_id INTEGER UNIQUE,
    twitch_id VARCHAR(50),
    name VARCHAR(255) NOT NULL,
    slug VARCHAR(255),
    release_date DATE,
    metacritic_score INTEGER,
    rating DECIMAL(3,2),
    playtime_hours INTEGER,
    is_indie BOOLEAN DEFAULT FALSE,
    is_live_service BOOLEAN DEFAULT FALSE,
    esrb_rating VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for common queries
CREATE INDEX idx_dim_games_name ON dim_games(name);
CREATE INDEX idx_dim_games_rawg_id ON dim_games(rawg_id);
CREATE INDEX idx_dim_games_twitch_id ON dim_games(twitch_id);
CREATE INDEX idx_dim_games_release_date ON dim_games(release_date);
CREATE INDEX idx_dim_games_is_live_service ON dim_games(is_live_service);

-- =============================================================================
-- Genres Dimension Table
-- =============================================================================
CREATE TABLE dim_genres (
    genre_id SERIAL PRIMARY KEY,
    rawg_genre_id INTEGER UNIQUE,
    name VARCHAR(100) NOT NULL,
    slug VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Index for lookups
CREATE INDEX idx_dim_genres_name ON dim_genres(name);

-- =============================================================================
-- Bridge Table: Games <-> Genres (Many-to-Many)
-- =============================================================================
CREATE TABLE bridge_game_genres (
    game_id INTEGER REFERENCES dim_games(game_id) ON DELETE CASCADE,
    genre_id INTEGER REFERENCES dim_genres(genre_id) ON DELETE CASCADE,
    PRIMARY KEY (game_id, genre_id)
);

-- =============================================================================
-- Trigger: Update timestamp on modification
-- =============================================================================
CREATE OR REPLACE FUNCTION update_modified_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER update_dim_games_modtime
    BEFORE UPDATE ON dim_games
    FOR EACH ROW
    EXECUTE FUNCTION update_modified_column();

-- =============================================================================
-- Verification
-- =============================================================================
SELECT table_name 
FROM information_schema.tables 
WHERE table_schema = 'public' 
AND table_name LIKE 'dim_%';
