"""
Sentiment Analyzer
Analyzes sentiment of Reddit posts and comments using VADER.

Why VADER?
- Specifically designed for social media text
- Handles slang, emojis, and casual language
- No training required - works out of the box
- Fast and lightweight

Usage:
    from transformers.sentiment_analyzer import SentimentAnalyzer
    
    analyzer = SentimentAnalyzer()
    score = analyzer.analyze_text("This game is absolutely amazing!")
    analyzer.analyze_reddit_posts()
"""

import os
import psycopg2
from typing import Optional, List, Tuple

# Try to import VADER
try:
    from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
    VADER_AVAILABLE = True
except ImportError:
    VADER_AVAILABLE = False


class SentimentAnalyzer:
    """
    Analyzes sentiment of text using VADER (Valence Aware Dictionary and sEntiment Reasoner).
    
    VADER is particularly good for:
    - Social media text (Reddit, Twitter, etc.)
    - Short informal text
    - Text with emojis and slang
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
        Initialize the analyzer.
        
        Args:
            host, port, dbname, user, password: Database connection params
        """
        if not VADER_AVAILABLE:
            raise ImportError(
                "VADER sentiment library required. Install with:\n"
                "  pip install vaderSentiment"
            )
        
        self.connection_params = {
            "host": host or os.environ.get("ANALYTICS_DB_HOST", "localhost"),
            "port": port or os.environ.get("ANALYTICS_DB_PORT", "5432"),
            "dbname": dbname or os.environ.get("ANALYTICS_DB_NAME", "gaming_analytics"),
            "user": user or os.environ.get("ANALYTICS_DB_USER", "analytics"),
            "password": password or os.environ.get("ANALYTICS_DB_PASSWORD", "analytics123")
        }
        
        self.conn = None
        self.cursor = None
        
        # Initialize VADER
        self.vader = SentimentIntensityAnalyzer()
        
        print("SentimentAnalyzer initialized (using VADER)")
    
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
    
    def analyze_text(self, text: str) -> dict:
        """
        Analyze sentiment of a single text.
        
        Args:
            text: Text to analyze
            
        Returns:
            Dictionary with sentiment scores:
            - compound: Overall sentiment (-1 to 1)
            - pos: Positive proportion (0 to 1)
            - neg: Negative proportion (0 to 1)
            - neu: Neutral proportion (0 to 1)
        """
        if not text or not text.strip():
            return {"compound": 0.0, "pos": 0.0, "neg": 0.0, "neu": 1.0}
        
        scores = self.vader.polarity_scores(text)
        return scores
    
    def get_sentiment_label(self, compound_score: float) -> str:
        """
        Convert compound score to a label.
        
        Args:
            compound_score: VADER compound score (-1 to 1)
            
        Returns:
            'positive', 'negative', or 'neutral'
        """
        if compound_score >= 0.05:
            return "positive"
        elif compound_score <= -0.05:
            return "negative"
        else:
            return "neutral"
    
    def analyze_batch(self, texts: List[str]) -> List[dict]:
        """
        Analyze sentiment of multiple texts.
        
        Args:
            texts: List of texts to analyze
            
        Returns:
            List of sentiment dictionaries
        """
        results = []
        
        for text in texts:
            scores = self.analyze_text(text)
            scores["label"] = self.get_sentiment_label(scores["compound"])
            results.append(scores)
        
        return results
    
    def analyze_reddit_posts(self, batch_size: int = 1000) -> int:
        """
        Analyze sentiment of Reddit posts in stg_reddit and update sentiment_score.
        
        Uses title + selftext for analysis.
        
        Args:
            batch_size: Number of posts to process at a time
            
        Returns:
            Number of posts analyzed
        """
        self.connect()
        
        print("\nAnalyzing Reddit post sentiment...")
        
        # Get posts without sentiment scores
        self.cursor.execute("""
            SELECT post_id, title, selftext 
            FROM stg_reddit 
            WHERE title IS NOT NULL
        """)
        
        posts = self.cursor.fetchall()
        total = len(posts)
        
        print(f"  Found {total} posts to analyze")
        
        analyzed = 0
        positive = 0
        negative = 0
        neutral = 0
        
        for post_id, title, selftext in posts:
            # Combine title and body for analysis
            text = title or ""
            if selftext:
                text += " " + selftext
            
            # Analyze
            scores = self.analyze_text(text)
            compound = scores["compound"]
            label = self.get_sentiment_label(compound)
            
            # Update database
            self.cursor.execute("""
                UPDATE stg_reddit 
                SET sentiment_score = %s
                WHERE post_id = %s
            """, (compound, post_id))
            
            # Count labels
            if label == "positive":
                positive += 1
            elif label == "negative":
                negative += 1
            else:
                neutral += 1
            
            analyzed += 1
            
            # Progress update
            if analyzed % 500 == 0:
                print(f"    Processed {analyzed}/{total} posts...")
                self.conn.commit()
        
        self.conn.commit()
        
        print(f"\n  ✓ Analyzed {analyzed} posts")
        print(f"    Sentiment distribution:")
        print(f"      Positive: {positive} ({positive/analyzed*100:.1f}%)")
        print(f"      Negative: {negative} ({negative/analyzed*100:.1f}%)")
        print(f"      Neutral:  {neutral} ({neutral/analyzed*100:.1f}%)")
        
        return analyzed
    
    def get_sentiment_summary(self) -> dict:
        """
        Get summary statistics of sentiment across all Reddit data.
        
        Returns:
            Dictionary with sentiment statistics
        """
        self.connect()
        
        # Posts sentiment
        self.cursor.execute("""
            SELECT 
                COUNT(*) as total,
                AVG(sentiment_score) as avg_score,
                COUNT(CASE WHEN sentiment_score >= 0.05 THEN 1 END) as positive,
                COUNT(CASE WHEN sentiment_score <= -0.05 THEN 1 END) as negative,
                COUNT(CASE WHEN sentiment_score > -0.05 AND sentiment_score < 0.05 THEN 1 END) as neutral
            FROM stg_reddit
            WHERE sentiment_score IS NOT NULL
        """)
        
        row = self.cursor.fetchone()
        
        posts_summary = {
            "total": row[0],
            "avg_score": float(row[1]) if row[1] else 0,
            "positive": row[2],
            "negative": row[3],
            "neutral": row[4]
        }
        
        # Comments sentiment (from Kaggle labeled data)
        self.cursor.execute("""
            SELECT 
                COUNT(*) as total,
                AVG(sentiment_score) as avg_score,
                COUNT(CASE WHEN sentiment_label = 'positive' THEN 1 END) as positive,
                COUNT(CASE WHEN sentiment_label = 'negative' THEN 1 END) as negative,
                COUNT(CASE WHEN sentiment_label = 'neutral' THEN 1 END) as neutral
            FROM stg_reddit_comments
        """)
        
        row = self.cursor.fetchone()
        
        comments_summary = {
            "total": row[0],
            "avg_score": float(row[1]) if row[1] else 0,
            "positive": row[2],
            "negative": row[3],
            "neutral": row[4]
        }
        
        return {
            "posts": posts_summary,
            "comments": comments_summary
        }
    
    def analyze_text_detailed(self, text: str) -> dict:
        """
        Get detailed sentiment analysis with explanation.
        
        Args:
            text: Text to analyze
            
        Returns:
            Detailed analysis dictionary
        """
        scores = self.analyze_text(text)
        
        return {
            "text": text[:100] + "..." if len(text) > 100 else text,
            "compound_score": scores["compound"],
            "label": self.get_sentiment_label(scores["compound"]),
            "positive_ratio": scores["pos"],
            "negative_ratio": scores["neg"],
            "neutral_ratio": scores["neu"],
            "interpretation": self._interpret_score(scores["compound"])
        }
    
    def _interpret_score(self, compound: float) -> str:
        """Generate human-readable interpretation of sentiment score."""
        if compound >= 0.5:
            return "Strongly positive"
        elif compound >= 0.05:
            return "Slightly positive"
        elif compound <= -0.5:
            return "Strongly negative"
        elif compound <= -0.05:
            return "Slightly negative"
        else:
            return "Neutral"


# =============================================================================
# Standalone execution for testing
# =============================================================================
if __name__ == "__main__":
    print("\n" + "=" * 50)
    print("  SENTIMENT ANALYZER TEST")
    print("=" * 50)
    
    analyzer = SentimentAnalyzer()
    
    # Test 1: Analyze sample texts
    print("\n[1/3] Testing sentiment analysis on sample texts...")
    
    test_texts = [
        "This game is absolutely amazing! Best RPG I've ever played!",
        "Terrible game, waste of money. Bugs everywhere.",
        "The game is okay, nothing special but not bad either.",
        "I love how the devs keep updating the game with free content!",
        "Another cash grab with overpriced DLC. Greedy developers.",
        "Just finished the main story. What a journey! 10/10",
        "Servers are down AGAIN. This is unacceptable.",
        "Graphics are decent but the gameplay is boring af"
    ]
    
    print("\n  Sample Analysis:")
    print("-" * 70)
    
    for text in test_texts:
        result = analyzer.analyze_text_detailed(text)
        print(f"  Text: {result['text']}")
        print(f"  Score: {result['compound_score']:.3f} ({result['label']}) - {result['interpretation']}")
        print()
    
    # Test 2: Analyze Reddit posts from database
    print("\n[2/3] Analyzing Reddit posts from database...")
    try:
        count = analyzer.analyze_reddit_posts()
    except Exception as e:
        print(f"  ✗ Error: {e}")
        print("  Make sure PostgreSQL is running and has Reddit data loaded.")
    
    # Test 3: Get summary
    print("\n[3/3] Getting sentiment summary...")
    try:
        summary = analyzer.get_sentiment_summary()
        
        print("\n  Posts Sentiment Summary:")
        print(f"    Total analyzed: {summary['posts']['total']}")
        print(f"    Average score: {summary['posts']['avg_score']:.3f}")
        print(f"    Positive: {summary['posts']['positive']}")
        print(f"    Negative: {summary['posts']['negative']}")
        print(f"    Neutral: {summary['posts']['neutral']}")
        
        print("\n  Comments Sentiment Summary (Kaggle labeled):")
        print(f"    Total: {summary['comments']['total']}")
        print(f"    Positive: {summary['comments']['positive']}")
        print(f"    Negative: {summary['comments']['negative']}")
        print(f"    Neutral: {summary['comments']['neutral']}")
        
    except Exception as e:
        print(f"  ✗ Error: {e}")
    
    analyzer.disconnect()
    print("\n✓ Sentiment Analyzer test complete!")
