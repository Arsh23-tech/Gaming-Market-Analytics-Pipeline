"""
Gaming Analytics Pipeline - Airflow DAG

This DAG orchestrates the entire ETL pipeline:
1. Extract data from RAWG API, Twitch API, and Reddit CSV files
2. Load extracted data into PostgreSQL staging tables
3. Transform data: match games, analyze sentiment, calculate metrics
4. Validate data quality

Schedule: Weekly (can be triggered manually for testing)

Author: Arsh
Project: Gaming Market Analytics Pipeline
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
import os
import sys

# Add src directory to path for imports
sys.path.insert(0, '/opt/airflow')

# =============================================================================
# Default Arguments
# =============================================================================
default_args = {
    'owner': 'arsh',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=1),
}

# =============================================================================
# Task Functions
# =============================================================================

def extract_rawg_games(**context):
    """Extract games from RAWG API."""
    from src.extractors.rawg_extractor import RAWGExtractor
    
    print("=" * 50)
    print("TASK: Extract RAWG Games")
    print("=" * 50)
    
    extractor = RAWGExtractor()
    
    # Extract games from the last 5 years with decent ratings
    games = extractor.extract_games(
        page_size=40,
        max_pages=10,  # ~400 games
        dates_from='2019-01-01',
        metacritic_min=None,  # Get all games, not just high-rated
        ordering='-rating'
    )
    
    # Save to staging
    output_path = '/opt/airflow/data/staging/rawg_games.json'
    extractor.save_to_json(games, output_path)
    
    # Push count to XCom for downstream tasks
    context['ti'].xcom_push(key='rawg_game_count', value=len(games))
    
    return f"Extracted {len(games)} games from RAWG"


def extract_twitch_top_games(**context):
    """Extract top games from Twitch API."""
    from src.extractors.twitch_extractor import TwitchExtractor
    
    print("=" * 50)
    print("TASK: Extract Twitch Top Games")
    print("=" * 50)
    
    extractor = TwitchExtractor()
    
    # Get top 100 games (excluding non-game categories)
    games = extractor.extract_top_games(limit=100, include_non_games=False)
    
    # Save to staging
    output_path = '/opt/airflow/data/staging/twitch_top_games.json'
    extractor.save_to_json(games, output_path)
    
    # Push count to XCom
    context['ti'].xcom_push(key='twitch_game_count', value=len(games))
    
    return f"Extracted {len(games)} top games from Twitch"


def extract_reddit_data(**context):
    """Load Reddit data from CSV files."""
    from src.extractors.reddit_loader import RedditLoader
    
    print("=" * 50)
    print("TASK: Extract Reddit Data")
    print("=" * 50)
    
    loader = RedditLoader(data_dir='/opt/airflow/data/raw')
    
    # Load posts
    posts = loader.load_gaming_posts()
    loader.save_to_json(posts, '/opt/airflow/data/staging/reddit_posts.json', 'posts')
    
    # Load sentiment comments
    comments = loader.load_sentiment_comments()
    loader.save_to_json(comments, '/opt/airflow/data/staging/reddit_comments.json', 'comments')
    
    # Push counts to XCom
    context['ti'].xcom_push(key='reddit_post_count', value=len(posts))
    context['ti'].xcom_push(key='reddit_comment_count', value=len(comments))
    
    return f"Loaded {len(posts)} posts and {len(comments)} comments from Reddit"


def load_rawg_to_staging(**context):
    """Load RAWG data into PostgreSQL staging table."""
    from src.loaders.postgres_loader import PostgresLoader
    
    print("=" * 50)
    print("TASK: Load RAWG to Staging")
    print("=" * 50)
    
    loader = PostgresLoader()
    count = loader.load_rawg_games('/opt/airflow/data/staging/rawg_games.json')
    loader.disconnect()
    
    return f"Loaded {count} games into stg_rawg"


def load_twitch_to_staging(**context):
    """Load Twitch data into PostgreSQL staging table."""
    from src.loaders.postgres_loader import PostgresLoader
    
    print("=" * 50)
    print("TASK: Load Twitch to Staging")
    print("=" * 50)
    
    loader = PostgresLoader()
    count = loader.load_twitch_snapshots('/opt/airflow/data/staging/twitch_top_games.json')
    loader.disconnect()
    
    return f"Loaded {count} snapshots into stg_twitch"


def load_reddit_to_staging(**context):
    """Load Reddit data into PostgreSQL staging tables."""
    from src.loaders.postgres_loader import PostgresLoader
    
    print("=" * 50)
    print("TASK: Load Reddit to Staging")
    print("=" * 50)
    
    loader = PostgresLoader()
    
    # Load posts
    post_count = loader.load_reddit_posts('/opt/airflow/data/staging/reddit_posts.json')
    
    # Load comments
    comment_count = loader.load_reddit_comments('/opt/airflow/data/staging/reddit_comments.json')
    
    loader.disconnect()
    
    return f"Loaded {post_count} posts and {comment_count} comments into staging"


def transform_match_games(**context):
    """Match games across data sources and populate dim_games."""
    from src.transformers.game_matcher import GameMatcher
    
    print("=" * 50)
    print("TASK: Match Games Across Sources")
    print("=" * 50)
    
    matcher = GameMatcher(match_threshold=80)
    
    # Load games from database
    matcher.load_games_from_db()
    
    # Match Twitch to RAWG
    matches = matcher.match_twitch_to_rawg()
    matched_count = sum(1 for m in matches if m['matched'])
    
    # Populate dim_games
    matcher.populate_dim_games_from_staging()
    
    # Update with Twitch mappings
    matcher.update_dim_games(matches)
    
    matcher.disconnect()
    
    # Push match rate to XCom
    match_rate = (matched_count / len(matches) * 100) if matches else 0
    context['ti'].xcom_push(key='game_match_rate', value=match_rate)
    
    return f"Matched {matched_count}/{len(matches)} games ({match_rate:.1f}%)"


def transform_analyze_sentiment(**context):
    """Analyze sentiment of Reddit posts."""
    from src.transformers.sentiment_analyzer import SentimentAnalyzer
    
    print("=" * 50)
    print("TASK: Analyze Sentiment")
    print("=" * 50)
    
    analyzer = SentimentAnalyzer()
    
    # Analyze Reddit posts
    count = analyzer.analyze_reddit_posts()
    
    # Get summary
    summary = analyzer.get_sentiment_summary()
    
    analyzer.disconnect()
    
    # Push sentiment stats to XCom
    context['ti'].xcom_push(key='avg_sentiment', value=summary['posts']['avg_score'])
    
    return f"Analyzed sentiment for {count} posts (avg: {summary['posts']['avg_score']:.3f})"


def transform_calculate_metrics(**context):
    """Calculate aggregated metrics and populate fact tables."""
    from src.transformers.metrics_calculator import MetricsCalculator
    
    print("=" * 50)
    print("TASK: Calculate Metrics")
    print("=" * 50)
    
    calculator = MetricsCalculator()
    
    # Run all transformations
    calculator.run_all_transformations()
    
    # Get summary stats
    stats = calculator.get_summary_stats()
    
    calculator.disconnect()
    
    # Push stats to XCom
    context['ti'].xcom_push(key='total_games', value=stats['total_games'])
    context['ti'].xcom_push(key='total_reddit_posts', value=stats['reddit_posts'])
    
    return f"Calculated metrics for {stats['total_games']} games"


def validate_data_quality(**context):
    """Validate data quality and generate report."""
    import psycopg2
    
    print("=" * 50)
    print("TASK: Validate Data Quality")
    print("=" * 50)
    
    conn = psycopg2.connect(
        host=os.environ.get("ANALYTICS_DB_HOST", "postgres-analytics"),
        port=os.environ.get("ANALYTICS_DB_PORT", "5432"),
        dbname=os.environ.get("ANALYTICS_DB_NAME", "gaming_analytics"),
        user=os.environ.get("ANALYTICS_DB_USER", "analytics"),
        password=os.environ.get("ANALYTICS_DB_PASSWORD", "analytics123")
    )
    cursor = conn.cursor()
    
    issues = []
    
    # Check 1: Games table has data
    cursor.execute("SELECT COUNT(*) FROM dim_games")
    game_count = cursor.fetchone()[0]
    if game_count == 0:
        issues.append("CRITICAL: dim_games is empty")
    else:
        print(f"  ✓ dim_games: {game_count} rows")
    
    # Check 2: Twitch snapshots exist
    cursor.execute("SELECT COUNT(*) FROM fact_twitch_snapshots")
    twitch_count = cursor.fetchone()[0]
    if twitch_count == 0:
        issues.append("WARNING: fact_twitch_snapshots is empty")
    else:
        print(f"  ✓ fact_twitch_snapshots: {twitch_count} rows")
    
    # Check 3: Reddit posts exist
    cursor.execute("SELECT COUNT(*) FROM fact_reddit_posts")
    reddit_count = cursor.fetchone()[0]
    if reddit_count == 0:
        issues.append("WARNING: fact_reddit_posts is empty")
    else:
        print(f"  ✓ fact_reddit_posts: {reddit_count} rows")
    
    # Check 4: Sentiment scores are populated
    cursor.execute("SELECT COUNT(*) FROM fact_reddit_posts WHERE sentiment_score IS NULL")
    null_sentiment = cursor.fetchone()[0]
    if null_sentiment > reddit_count * 0.1:  # More than 10% null
        issues.append(f"WARNING: {null_sentiment} posts missing sentiment scores")
    else:
        print(f"  ✓ Sentiment coverage: {reddit_count - null_sentiment}/{reddit_count}")
    
    # Check 5: Metrics are calculated
    cursor.execute("SELECT COUNT(*) FROM agg_game_metrics")
    metrics_count = cursor.fetchone()[0]
    if metrics_count == 0:
        issues.append("WARNING: agg_game_metrics is empty")
    else:
        print(f"  ✓ agg_game_metrics: {metrics_count} rows")
    
    cursor.close()
    conn.close()
    
    # Report
    print("\n" + "-" * 50)
    if issues:
        print("Data Quality Issues Found:")
        for issue in issues:
            print(f"  - {issue}")
        if any("CRITICAL" in i for i in issues):
            raise ValueError("Critical data quality issues found!")
    else:
        print("✓ All data quality checks passed!")
    
    return f"Validation complete: {len(issues)} issues found"


def generate_summary_report(**context):
    """Generate a summary report of the pipeline run."""
    ti = context['ti']
    
    print("=" * 50)
    print("PIPELINE SUMMARY REPORT")
    print("=" * 50)
    
    # Pull XCom values
    rawg_count = ti.xcom_pull(key='rawg_game_count', task_ids='extract.extract_rawg') or 'N/A'
    twitch_count = ti.xcom_pull(key='twitch_game_count', task_ids='extract.extract_twitch') or 'N/A'
    reddit_posts = ti.xcom_pull(key='reddit_post_count', task_ids='extract.extract_reddit') or 'N/A'
    match_rate = ti.xcom_pull(key='game_match_rate', task_ids='transform.match_games') or 'N/A'
    avg_sentiment = ti.xcom_pull(key='avg_sentiment', task_ids='transform.analyze_sentiment') or 'N/A'
    total_games = ti.xcom_pull(key='total_games', task_ids='transform.calculate_metrics') or 'N/A'
    
    print(f"""
    Extraction:
      - RAWG Games: {rawg_count}
      - Twitch Top Games: {twitch_count}
      - Reddit Posts: {reddit_posts}
    
    Transformation:
      - Game Match Rate: {match_rate}%
      - Average Sentiment: {avg_sentiment}
      - Total Games in DB: {total_games}
    
    Pipeline completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
    """)
    
    return "Summary report generated"


# =============================================================================
# DAG Definition
# =============================================================================
with DAG(
    dag_id='gaming_analytics_pipeline',
    default_args=default_args,
    description='ETL pipeline for gaming market analytics',
    schedule_interval='@weekly',  # Run weekly
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['gaming', 'analytics', 'etl'],
) as dag:
    
    # Start and End markers
    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')
    
    # -------------------------------------------------------------------------
    # Extract Task Group
    # -------------------------------------------------------------------------
    with TaskGroup(group_id='extract') as extract_group:
        
        extract_rawg = PythonOperator(
            task_id='extract_rawg',
            python_callable=extract_rawg_games,
        )
        
        extract_twitch = PythonOperator(
            task_id='extract_twitch',
            python_callable=extract_twitch_top_games,
        )
        
        extract_reddit = PythonOperator(
            task_id='extract_reddit',
            python_callable=extract_reddit_data,
        )
    
    # -------------------------------------------------------------------------
    # Load Task Group
    # -------------------------------------------------------------------------
    with TaskGroup(group_id='load') as load_group:
        
        load_rawg = PythonOperator(
            task_id='load_rawg',
            python_callable=load_rawg_to_staging,
        )
        
        load_twitch = PythonOperator(
            task_id='load_twitch',
            python_callable=load_twitch_to_staging,
        )
        
        load_reddit = PythonOperator(
            task_id='load_reddit',
            python_callable=load_reddit_to_staging,
        )
    
    # -------------------------------------------------------------------------
    # Transform Task Group
    # -------------------------------------------------------------------------
    with TaskGroup(group_id='transform') as transform_group:
        
        match_games = PythonOperator(
            task_id='match_games',
            python_callable=transform_match_games,
        )
        
        analyze_sentiment = PythonOperator(
            task_id='analyze_sentiment',
            python_callable=transform_analyze_sentiment,
        )
        
        calculate_metrics = PythonOperator(
            task_id='calculate_metrics',
            python_callable=transform_calculate_metrics,
        )
        
        # Sentiment and metrics can run in parallel after matching
        match_games >> [analyze_sentiment, calculate_metrics]
    
    # -------------------------------------------------------------------------
    # Validate and Report
    # -------------------------------------------------------------------------
    validate = PythonOperator(
        task_id='validate_data_quality',
        python_callable=validate_data_quality,
    )
    
    report = PythonOperator(
        task_id='generate_report',
        python_callable=generate_summary_report,
    )
    
    # -------------------------------------------------------------------------
    # Task Dependencies
    # -------------------------------------------------------------------------
    # Extract tasks run in parallel
    start >> extract_group
    
    # Load tasks depend on their respective extract tasks
    extract_rawg >> load_rawg
    extract_twitch >> load_twitch
    extract_reddit >> load_reddit
    
    # Transform starts after all loads complete
    [load_rawg, load_twitch, load_reddit] >> transform_group
    
    # Validate after transform
    transform_group >> validate
    
    # Report after validation
    validate >> report >> end
