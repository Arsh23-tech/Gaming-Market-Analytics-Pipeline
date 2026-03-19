# Gaming Market Analytics Pipeline

A production-ready ETL pipeline that analyzes gaming market trends by integrating data from RAWG (game metadata), Twitch (streaming viewership), and Reddit (community sentiment). Built with Apache Airflow for orchestration and Power BI for visualization.

![Dashboard Preview](dashboard_preview.png)

---

## Business Questions Answered

| Question | Finding |
|----------|---------|
| **Where is player attention concentrated?** | Top 10 games capture the majority of 7.3M+ Twitch viewers |
| **Do highly-rated games get watched?** | Not always—live-service games dominate viewership regardless of ratings |
| **What's the market composition?** | 81% Single-Player vs 19% Live-Service games |
| **How is community sentiment?** | Healthy—52% positive, 28% neutral, 20% negative |

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                         DATA SOURCES                                │
├─────────────────┬─────────────────┬─────────────────────────────────┤
│   RAWG API      │   Twitch API    │   Reddit (Kaggle CSV)           │
│   Game Metadata │   Live Viewers  │   Community Sentiment           │
└────────┬────────┴────────┬────────┴─────────────────┬───────────────┘
         │                 │                          │
         ▼                 ▼                          ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    APACHE AIRFLOW (Orchestration)                   │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐                  │
│  │   Extract   │──│    Load     │──│  Transform  │                  │
│  │   (3 tasks) │  │  (3 tasks)  │  │  (3 tasks)  │                  │
│  └─────────────┘  └─────────────┘  └─────────────┘                  │
└─────────────────────────────────────────────────────────────────────┘
         │                 │                          │
         ▼                 ▼                          ▼
┌─────────────────────────────────────────────────────────────────────┐
│                      POSTGRESQL (Data Warehouse)                    │
│  ┌─────────────┐  ┌─────────────────┐  ┌─────────────────────────┐  │
│  │  Staging    │  │   Dimension     │  │        Fact             │  │
│  │  Tables     │──│   Tables        │──│       Tables            │  │
│  │  (stg_*)    │  │   (dim_games)   │  │  (fact_twitch,reddit)   │  │
│  └─────────────┘  └─────────────────┘  └─────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────────┐
│                     POWER BI (Visualization)                        │
│         Interactive Dashboard with 7 Visualizations                 │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Tech Stack

| Layer | Technology | Purpose |
|-------|------------|---------|
| **Extraction** | Python, Requests | API calls to RAWG & Twitch |
| **Orchestration** | Apache Airflow 2.8 | DAG scheduling, task dependencies |
| **Storage** | PostgreSQL 15 | Data warehouse with star schema |
| **Transformation** | Python, SQL | Fuzzy matching, sentiment analysis |
| **Containerization** | Docker, Docker Compose | Reproducible environment |
| **Visualization** | Power BI | Interactive dashboards |

---

## Project Structure

```
gaming-analytics-pipeline/
├── dags/
│   └── gaming_analytics_dag.py      # Airflow DAG definition
├── src/
│   ├── extractors/
│   │   ├── rawg_extractor.py        # RAWG API client
│   │   ├── twitch_extractor.py      # Twitch API client
│   │   └── reddit_loader.py         # Reddit CSV loader
│   ├── loaders/
│   │   └── postgres_loader.py       # Database loader
│   └── transformers/
│       ├── game_matcher.py          # Fuzzy matching (RapidFuzz)
│       ├── sentiment_analyzer.py    # VADER sentiment analysis
│       └── metrics_calculator.py    # Aggregation & metrics
├── sql/
│   └── schema/                      # DDL scripts for all tables
├── data/
│   ├── raw/                         # Source CSV files
│   └── staging/                     # Intermediate JSON files
├── docker-compose.yml               # Container orchestration
├── Dockerfile                       # Custom Airflow image
├── requirements.txt                 # Python dependencies
└── README.md
```

---

## Getting Started

### Prerequisites

- Docker & Docker Compose
- Python 3.11+
- Power BI Desktop (for visualization)
- API Keys: [RAWG](https://rawg.io/apidocs) and [Twitch](https://dev.twitch.tv/console)

### Installation

1. **Clone the repository**
   ```bash
   git clone https://github.com/yourusername/gaming-analytics-pipeline.git
   cd gaming-analytics-pipeline
   ```

2. **Configure environment variables**
   ```bash
   cp .env.example .env
   # Edit .env with your API keys
   ```

3. **Start the containers**
   ```bash
   docker-compose build
   docker-compose up -d
   ```

4. **Access Airflow UI**
   - URL: http://localhost:8080
   - Credentials: airflow / airflow

5. **Trigger the DAG**
   - Enable `gaming_analytics_pipeline` DAG
   - Click "Trigger DAG" to run manually

6. **Connect Power BI**
   - Server: `localhost`
   - Database: `gaming_analytics`
   - User: `analytics` / Password: `analytics123`

---

## Key Features

### 1. Intelligent Game Matching
Uses **RapidFuzz** for fuzzy string matching to link games across different data sources with varying naming conventions.

```python
# Example: Matches "Counter-Strike" → "Counter-Strike 2" (93% confidence)
matcher = GameMatcher(match_threshold=80)
matches = matcher.match_twitch_to_rawg()
```

### 2. Sentiment Analysis
Leverages **VADER** (Valence Aware Dictionary and sEntiment Reasoner) for gaming-specific sentiment scoring.

```python
# Analyzes 1,876 Reddit posts
# Result: 52% positive, 28% neutral, 20% negative
analyzer = SentimentAnalyzer()
analyzer.analyze_reddit_posts()
```

### 3. Automated Data Quality Checks
Built-in validation ensures data integrity before visualization.

```python
# Validates: row counts, null percentages, referential integrity
validate_data_quality()
```

---

## Dashboard Visualizations

| Visualization | Type | Insight |
|---------------|------|---------|
| **Games Analyzed** | KPI Card | 591 games in dataset |
| **Total Twitch Viewers** | KPI Card | 7.3M+ total viewership |
| **Positive/Negative Sentiment** | KPI Cards | 52% positive, 20% negative |
| **Top 10 by Viewership** | Bar Chart | Rocket League, Minecraft, GTA V lead |
| **Rating vs Viewership** | Scatter Plot | High ratings ≠ high viewership |
| **Games by Type** | Column Chart | 81% Single-Player, 19% Live-Service |
| **Sentiment Breakdown** | Column Chart | Community health metrics |
| **Performance Scorecard** | Table | Sortable game metrics |

---

## Key Insights

1. **Market Concentration**: The top 10 games capture the vast majority of streaming viewership, indicating a "winner-take-all" dynamic in player attention.

2. **Live-Service Dominance**: Despite representing only 19% of games analyzed, live-service titles dominate the top viewership rankings (8 of top 10).

3. **Quality ≠ Popularity**: The scatter plot reveals that critically acclaimed single-player games often have lower viewership than mediocre live-service games.

4. **Healthy Community Sentiment**: With 52% positive sentiment, the gaming community shows healthy engagement levels despite industry challenges.

---

## Testing

```bash
# Run individual extractors
python src/extractors/rawg_extractor.py
python src/extractors/twitch_extractor.py

# Run transformers
python src/transformers/game_matcher.py
python src/transformers/sentiment_analyzer.py
python src/transformers/metrics_calculator.py
```

---

## Data Sources

| Source | Type | Records | Description |
|--------|------|---------|-------------|
| **RAWG API** | REST API | ~400 games | Game metadata, ratings, genres |
| **Twitch API** | REST API | Top 100 | Live streaming data |
| **Reddit r/gaming** | Kaggle CSV | 1,876 posts | Community discussions |
| **Reddit Comments** | Kaggle CSV | 23,189 comments | Sentiment-labeled data |

---

## Future Enhancements

- [ ] Add Steam API for player count data
- [ ] Implement incremental loading for historical trends
- [ ] Add genre-level analysis
- [ ] Create automated email reports
- [ ] Deploy to cloud (AWS/GCP)

---

## Author

**Arsh Chandrakar**
- MS in Information Systems, Syracuse University
- Lean Six Sigma Green Belt
- [LinkedIn](https://www.linkedin.com/in/arsh-chandrakar/)
- [Portfolio](https://arsh23-tech.github.io/)

---

## License

This project is licensed under the MIT License.

---

## Acknowledgments

- [RAWG Video Games Database](https://rawg.io/) for comprehensive game metadata
- [Twitch API](https://dev.twitch.tv/) for streaming data
- [Kaggle](https://www.kaggle.com/) for Reddit gaming datasets
