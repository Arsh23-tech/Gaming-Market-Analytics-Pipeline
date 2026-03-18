# 🏗️ Technical Architecture

This document provides a deep-dive into the technical implementation of the Gaming Market Analytics Pipeline.

---

## Table of Contents

1. [System Overview](#system-overview)
2. [Data Flow](#data-flow)
3. [Database Schema](#database-schema)
4. [Airflow DAG](#airflow-dag)
5. [Transformation Logic](#transformation-logic)
6. [Docker Configuration](#docker-configuration)

---

## System Overview

The pipeline follows a classic **ETL (Extract, Transform, Load)** architecture with modern tooling:

```
┌──────────────────────────────────────────────────────────────────┐
│                        EXTRACTION LAYER                          │
│  ┌────────────────┐  ┌────────────────┐  ┌────────────────┐      │
│  │ RAWG Extractor │  │Twitch Extractor│  │ Reddit Loader  │      │
│  │   (API)        │  │    (API)       │  │   (CSV)        │      │
│  └───────┬────────┘  └───────┬────────┘  └───────┬────────┘      │
│          │                   │                   │               │
│          ▼                   ▼                   ▼               │
│  ┌─────────────────────────────────────────────────────────┐     │
│  │              JSON Staging Files (data/staging/)         │     │
│  └─────────────────────────────────────────────────────────┘     │
└──────────────────────────────────────────────────────────────────┘
                               │
                               ▼
┌──────────────────────────────────────────────────────────────────┐
│                         LOADING LAYER                            │
│  ┌─────────────────────────────────────────────────────────┐     │
│  │              PostgreSQL Staging Tables                  │     │
│  │         (stg_rawg, stg_twitch, stg_reddit)              │     │
│  └─────────────────────────────────────────────────────────┘     │
└──────────────────────────────────────────────────────────────────┘
                               │
                               ▼
┌──────────────────────────────────────────────────────────────────┐
│                     TRANSFORMATION LAYER                         │
│  ┌────────────────┐  ┌────────────────┐  ┌────────────────┐      │
│  │  Game Matcher  │  │   Sentiment    │  │    Metrics     │      │
│  │  (RapidFuzz)   │  │   Analyzer     │  │   Calculator   │      │
│  │                │  │   (VADER)      │  │                │      │
│  └───────┬────────┘  └───────┬────────┘  └───────┬────────┘      │
│          │                   │                   │               │
│          ▼                   ▼                   ▼               │
│  ┌─────────────────────────────────────────────────────────┐     │
│  │     Dimension & Fact Tables (Star Schema)               │     │
│  │  (dim_games, fact_twitch_snapshots, fact_reddit_posts)  │     │
│  └─────────────────────────────────────────────────────────┘     │
└──────────────────────────────────────────────────────────────────┘
                               │
                               ▼
┌──────────────────────────────────────────────────────────────────┐
│                      PRESENTATION LAYER                          │
│  ┌─────────────────────────────────────────────────────────┐     │
│  │                    Power BI Dashboard                   │     │
│  │              (7 Interactive Visualizations)             │     │
│  └─────────────────────────────────────────────────────────┘     │
└──────────────────────────────────────────────────────────────────┘
```

---

## Data Flow

### 1. Extraction Phase

| Source | Extractor | Output | Records |
|--------|-----------|--------|---------|
| RAWG API | `rawg_extractor.py` | `rawg_games.json` | ~400 games |
| Twitch API | `twitch_extractor.py` | `twitch_top_games.json` | Top 100 |
| Reddit CSV | `reddit_loader.py` | `reddit_posts.json` | 1,876 posts |
| Reddit CSV | `reddit_loader.py` | `reddit_comments.json` | 23,189 comments |

### 2. Loading Phase

Data flows from JSON → PostgreSQL staging tables:

```sql
-- Staging tables receive raw data
stg_rawg          -- Game metadata from RAWG
stg_twitch        -- Streaming snapshots from Twitch
stg_reddit        -- Post data from Reddit
stg_reddit_comments -- Comment sentiment data
```

### 3. Transformation Phase

| Transformer | Input | Output | Logic |
|-------------|-------|--------|-------|
| Game Matcher | stg_rawg, stg_twitch | dim_games | Fuzzy matching with 80% threshold |
| Sentiment Analyzer | stg_reddit | fact_reddit_posts | VADER compound scoring |
| Metrics Calculator | All staging | agg_game_metrics | Aggregation & traction scores |

---

## Database Schema

### Star Schema Design

```
                    ┌─────────────────────┐
                    │     dim_games       │
                    │  ───────────────    │
                    │  game_id (PK)       │
                    │  rawg_id            │
                    │  twitch_id          │
                    │  name               │
                    │  release_date       │
                    │  metacritic_score   │
                    │  rating             │
                    │  is_indie           │
                    │  is_live_service    │
                    └──────────┬──────────┘
                               │
           ┌───────────────────┼───────────────────┐
           │                   │                   │
           ▼                   ▼                   ▼
┌─────────────────────┐ ┌─────────────────────┐ ┌─────────────────────┐
│ fact_twitch_snapshots│ │  fact_reddit_posts  │ │  agg_game_metrics   │
│  ───────────────────│ │  ─────────────────  │ │  ─────────────────  │
│  snapshot_id (PK)   │ │  post_id (PK)       │ │  game_id (PK, FK)   │
│  game_id (FK)       │ │  title              │ │  avg_twitch_viewers │
│  viewer_count       │ │  score              │ │  max_twitch_viewers │
│  stream_count       │ │  num_comments       │ │  reddit_mention_cnt │
│  snapshot_time      │ │  sentiment_score    │ │  avg_reddit_sentmnt │
│  rank_position      │ │  created_utc        │ │  traction_score     │
└─────────────────────┘ └─────────────────────┘ └─────────────────────┘
```

### Table Details

#### dim_games (Dimension)
```sql
CREATE TABLE dim_games (
    game_id SERIAL PRIMARY KEY,
    rawg_id INTEGER,
    twitch_id VARCHAR(50),
    name VARCHAR(255) NOT NULL,
    slug VARCHAR(255),
    release_date DATE,
    metacritic_score INTEGER,
    rating DECIMAL(3,2),
    playtime_hours INTEGER,
    is_indie BOOLEAN DEFAULT FALSE,
    is_live_service BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

#### fact_twitch_snapshots (Fact)
```sql
CREATE TABLE fact_twitch_snapshots (
    snapshot_id SERIAL PRIMARY KEY,
    game_id INTEGER REFERENCES dim_games(game_id),
    twitch_game_id VARCHAR(50),
    twitch_game_name VARCHAR(255),
    snapshot_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    viewer_count INTEGER,
    stream_count INTEGER,
    rank_position INTEGER
);
```

#### fact_reddit_posts (Fact)
```sql
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
    data_source VARCHAR(50)
);
```

---

## Airflow DAG

### DAG Structure

```
gaming_analytics_pipeline
│
├── start (EmptyOperator)
│
├── extract (TaskGroup)
│   ├── extract_rawg ──────────────┐
│   ├── extract_twitch ────────────┼── (parallel execution)
│   └── extract_reddit ────────────┘
│
├── load (TaskGroup)
│   ├── load_rawg ─────────────────┐
│   ├── load_twitch ───────────────┼── (parallel execution)
│   └── load_reddit ───────────────┘
│
├── transform (TaskGroup)
│   ├── match_games
│   │   ├── analyze_sentiment ─────┐
│   │   └── calculate_metrics ─────┘── (parallel after match)
│
├── validate_data_quality
│
├── generate_report
│
└── end (EmptyOperator)
```

### Task Dependencies

```python
# Extract tasks run in parallel
start >> extract_group

# Load depends on respective extract
extract_rawg >> load_rawg
extract_twitch >> load_twitch
extract_reddit >> load_reddit

# Transform starts after all loads complete
[load_rawg, load_twitch, load_reddit] >> transform_group

# Match must complete before sentiment and metrics (parallel)
match_games >> [analyze_sentiment, calculate_metrics]

# Validate after transform
transform_group >> validate >> report >> end
```

### DAG Configuration

```python
default_args = {
    'owner': 'arsh',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=1),
}

dag = DAG(
    dag_id='gaming_analytics_pipeline',
    schedule_interval='@weekly',
    start_date=datetime(2024, 1, 1),
    catchup=False,
)
```

---

## Transformation Logic

### 1. Game Matcher (Fuzzy Matching)

Uses RapidFuzz library for intelligent string matching:

```python
from rapidfuzz import fuzz

class GameMatcher:
    def __init__(self, match_threshold=80):
        self.match_threshold = match_threshold
    
    def match_twitch_to_rawg(self):
        for twitch_game in self.twitch_games:
            best_match = None
            best_score = 0
            
            for rawg_game in self.rawg_games:
                # Calculate similarity score
                score = fuzz.ratio(
                    twitch_game['name'].lower(),
                    rawg_game['name'].lower()
                )
                
                if score > best_score:
                    best_score = score
                    best_match = rawg_game
            
            if best_score >= self.match_threshold:
                # Matched!
                yield {
                    'twitch_game': twitch_game,
                    'rawg_game': best_match,
                    'confidence': best_score
                }
```

**Match Examples:**
| Twitch Name | RAWG Match | Confidence |
|-------------|------------|------------|
| Counter-Strike | Counter-Strike 2 | 93% |
| Overwatch | Overwatch 2 | 90% |
| VALORANT | Valorant | 100% |

### 2. Sentiment Analyzer (VADER)

Uses VADER (Valence Aware Dictionary and sEntiment Reasoner):

```python
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

class SentimentAnalyzer:
    def __init__(self):
        self.analyzer = SentimentIntensityAnalyzer()
    
    def analyze_text(self, text):
        scores = self.analyzer.polarity_scores(text)
        return scores['compound']  # Range: -1 to +1
    
    def categorize(self, score):
        if score >= 0.05:
            return 'positive'
        elif score <= -0.05:
            return 'negative'
        else:
            return 'neutral'
```

**Sentiment Distribution:**
| Category | Count | Percentage |
|----------|-------|------------|
| Positive | 980 | 52.2% |
| Neutral | 521 | 27.8% |
| Negative | 375 | 20.0% |

### 3. Metrics Calculator

Computes aggregated metrics and traction scores:

```python
class MetricsCalculator:
    def calculate_traction_score(self, game):
        """
        Traction Score = weighted combination of:
        - Twitch viewership (40%)
        - Reddit engagement (30%)
        - Metacritic score (20%)
        - User rating (10%)
        """
        twitch_score = normalize(game['avg_twitch_viewers'], max_viewers)
        reddit_score = normalize(game['reddit_mentions'], max_mentions)
        meta_score = game['metacritic_score'] / 100
        rating_score = game['rating'] / 5
        
        return (
            twitch_score * 0.4 +
            reddit_score * 0.3 +
            meta_score * 0.2 +
            rating_score * 0.1
        ) * 100
```

---

## Docker Configuration

### docker-compose.yml Structure

```yaml
version: '3.8'

x-airflow-common: &airflow-common
  build:
    context: .
    dockerfile: Dockerfile
  environment:
    - AIRFLOW__CORE__EXECUTOR=LocalExecutor
    - AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@postgres-airflow/airflow
    - RAWG_API_KEY=${RAWG_API_KEY}
    - TWITCH_CLIENT_ID=${TWITCH_CLIENT_ID}
    - TWITCH_CLIENT_SECRET=${TWITCH_CLIENT_SECRET}
  volumes:
    - ./dags:/opt/airflow/dags
    - ./src:/opt/airflow/src
    - ./data:/opt/airflow/data

services:
  postgres-airflow:
    image: postgres:15
    environment:
      POSTGRES_USER: airflow
      POSTGRES_PASSWORD: airflow
    ports:
      - "5433:5432"

  postgres-analytics:
    image: postgres:15
    environment:
      POSTGRES_USER: analytics
      POSTGRES_PASSWORD: analytics123
      POSTGRES_DB: gaming_analytics
    ports:
      - "5432:5432"

  airflow-webserver:
    <<: *airflow-common
    command: webserver
    ports:
      - "8080:8080"

  airflow-scheduler:
    <<: *airflow-common
    command: scheduler

  airflow-triggerer:
    <<: *airflow-common
    command: triggerer
```

### Custom Dockerfile

```dockerfile
FROM apache/airflow:2.8.0-python3.11

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
```

### Container Network

```
┌─────────────────────────────────────────────────────────────┐
│                    Docker Network                           │
│  ┌─────────────────┐      ┌──────────────────┐              │
│  │ postgres-airflow│      │postgres-analytics│              │
│  │   Port: 5433    │      │   Port: 5432     │              │
│  └────────┬────────┘      └────────┬─────────┘              │
│           │                        │                        │
│           ▼                        ▼                        │
│  ┌─────────────────────────────────────────────────────┐    │
│  │              Airflow Containers                     │    │
│  │  ┌──────────┐  ┌───────────┐  ┌───────────┐         │    │
│  │  │webserver │  │ scheduler │  │ triggerer │         │    │
│  │  │ :8080    │  │           │  │           │         │    │
│  │  └──────────┘  └───────────┘  └───────────┘         │    │
│  └─────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────┘
```

---

## Performance Considerations

### Current Metrics

| Metric | Value |
|--------|-------|
| Full DAG Runtime | ~3-5 minutes |
| RAWG Extraction | ~2 minutes (400 games) |
| Twitch Extraction | ~10 seconds (100 games) |
| Sentiment Analysis | ~30 seconds (1,876 posts) |
| Database Size | ~50 MB |

### Optimization Opportunities

1. **Incremental Loading**: Track last extraction timestamp to avoid re-processing
2. **Parallel Transformations**: Sentiment and metrics already run in parallel
3. **Connection Pooling**: Implement for high-frequency extractions
4. **Indexing**: Add indexes on frequently queried columns

---

## Security Considerations

1. **API Keys**: Stored in `.env` file (not committed to Git)
2. **Database Credentials**: Passed via environment variables
3. **Network Isolation**: Containers communicate via internal Docker network
4. **No External Exposure**: Only Airflow UI and PostgreSQL exposed to localhost
