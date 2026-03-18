"""
Reddit Data Loader
Loads and processes Reddit data from Kaggle CSV files.

Usage:
    from extractors.reddit_loader import RedditLoader
    
    loader = RedditLoader()
    posts = loader.load_gaming_posts("data/raw/gaming.csv")
    comments = loader.load_sentiment_comments("data/raw/23k_r_gaming_comments_sentiments.csv")
"""

import os
import csv
import json
from datetime import datetime
from typing import Optional


class RedditLoader:
    """
    Loads Reddit data from Kaggle CSV datasets.
    
    Supports two datasets:
    1. gaming.csv - Reddit posts/submissions with engagement metrics
    2. 23k_r_gaming_comments_sentiments.csv - Comments with pre-labeled sentiment
    """
    
    def __init__(self, data_dir: str = "data/raw"):
        """
        Initialize the loader.
        
        Args:
            data_dir: Directory containing the CSV files
        """
        self.data_dir = data_dir
    
    def load_gaming_posts(self, filepath: Optional[str] = None) -> list:
        """
        Load Reddit posts from gaming.csv dataset.
        
        Expected columns: title, score, id, url, comms_num, created, body, timestamp
        
        Args:
            filepath: Path to CSV file. If not provided, uses default location.
            
        Returns:
            List of post dictionaries
        """
        if filepath is None:
            filepath = os.path.join(self.data_dir, "gaming.csv")
        
        if not os.path.exists(filepath):
            raise FileNotFoundError(f"File not found: {filepath}")
        
        print(f"Loading Reddit posts from {filepath}...")
        
        posts = []
        errors = 0
        
        with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
            reader = csv.DictReader(f)
            
            for row in reader:
                try:
                    post = self._parse_gaming_post(row)
                    if post:
                        posts.append(post)
                except Exception as e:
                    errors += 1
                    if errors <= 5:  # Only show first 5 errors
                        print(f"  Warning: Error parsing row: {e}")
        
        print(f"  ✓ Loaded {len(posts):,} posts")
        if errors > 0:
            print(f"  ⚠ {errors} rows had parsing errors")
        
        return posts
    
    def _parse_gaming_post(self, row: dict) -> dict:
        """
        Parse a single row from gaming.csv into our format.
        
        Args:
            row: CSV row dictionary
            
        Returns:
            Parsed post dictionary
        """
        # Parse timestamp
        created_utc = None
        if row.get('created'):
            try:
                # The 'created' field appears to be Unix timestamp
                created_utc = datetime.fromtimestamp(float(row['created']))
            except (ValueError, TypeError):
                pass
        
        # Parse numeric fields
        score = self._safe_int(row.get('score'))
        comms_num = self._safe_int(row.get('comms_num'))
        
        return {
            "post_id": row.get('id', '').strip(),
            "subreddit": "gaming",  # This dataset is specifically r/gaming
            "title": row.get('title', '').strip(),
            "selftext": row.get('body', '').strip(),
            "score": score,
            "num_comments": comms_num,
            "created_utc": created_utc.isoformat() if created_utc else None,
            "url": row.get('url', '').strip(),
            "data_source": "kaggle_gaming"
        }
    
    def load_sentiment_comments(self, filepath: Optional[str] = None) -> list:
        """
        Load Reddit comments with sentiment labels from Kaggle dataset.
        
        Expected columns: Comment, sentiment
        
        Args:
            filepath: Path to CSV file. If not provided, uses default location.
            
        Returns:
            List of comment dictionaries with sentiment labels
        """
        if filepath is None:
            filepath = os.path.join(self.data_dir, "23k_r_gaming_comments_sentiments.csv")
        
        if not os.path.exists(filepath):
            raise FileNotFoundError(f"File not found: {filepath}")
        
        print(f"Loading sentiment comments from {filepath}...")
        
        comments = []
        sentiment_counts = {"positive": 0, "negative": 0, "neutral": 0}
        errors = 0
        
        with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
            reader = csv.DictReader(f)
            
            for i, row in enumerate(reader):
                try:
                    comment = self._parse_sentiment_comment(row, i)
                    if comment:
                        comments.append(comment)
                        sentiment = comment.get('sentiment_label', 'unknown')
                        if sentiment in sentiment_counts:
                            sentiment_counts[sentiment] += 1
                except Exception as e:
                    errors += 1
                    if errors <= 5:
                        print(f"  Warning: Error parsing row {i}: {e}")
        
        print(f"  ✓ Loaded {len(comments):,} comments")
        print(f"    Sentiment distribution:")
        for sentiment, count in sentiment_counts.items():
            pct = (count / len(comments) * 100) if comments else 0
            print(f"      {sentiment}: {count:,} ({pct:.1f}%)")
        
        if errors > 0:
            print(f"  ⚠ {errors} rows had parsing errors")
        
        return comments
    
    def _parse_sentiment_comment(self, row: dict, index: int) -> dict:
        """
        Parse a single row from sentiment comments CSV.
        
        Args:
            row: CSV row dictionary
            index: Row index for generating unique ID
            
        Returns:
            Parsed comment dictionary
        """
        # Get comment text - handle different possible column names
        comment_text = row.get('Comment') or row.get('comment') or row.get('text', '')
        
        # Get sentiment - normalize to lowercase
        sentiment = (row.get('sentiment') or row.get('Sentiment') or 'unknown').strip().lower()
        
        # Convert sentiment to numeric score for analysis
        sentiment_score = self._sentiment_to_score(sentiment)
        
        return {
            "comment_id": f"kaggle_sentiment_{index}",
            "subreddit": "gaming",
            "text": comment_text.strip(),
            "sentiment_label": sentiment,
            "sentiment_score": sentiment_score,
            "data_source": "kaggle_sentiment"
        }
    
    def _sentiment_to_score(self, sentiment: str) -> float:
        """
        Convert sentiment label to numeric score.
        
        Args:
            sentiment: Sentiment label (positive, negative, neutral)
            
        Returns:
            Numeric score (-1 to 1)
        """
        mapping = {
            "positive": 1.0,
            "negative": -1.0,
            "neutral": 0.0
        }
        return mapping.get(sentiment, 0.0)
    
    def _safe_int(self, value) -> int:
        """Safely convert value to integer."""
        if value is None:
            return 0
        try:
            return int(float(value))
        except (ValueError, TypeError):
            return 0
    
    def get_posts_summary(self, posts: list) -> dict:
        """
        Generate summary statistics for loaded posts.
        
        Args:
            posts: List of post dictionaries
            
        Returns:
            Summary statistics dictionary
        """
        if not posts:
            return {"count": 0}
        
        scores = [p['score'] for p in posts if p.get('score')]
        comments = [p['num_comments'] for p in posts if p.get('num_comments')]
        
        # Get date range
        dates = [p['created_utc'] for p in posts if p.get('created_utc')]
        
        return {
            "count": len(posts),
            "total_score": sum(scores),
            "avg_score": sum(scores) / len(scores) if scores else 0,
            "max_score": max(scores) if scores else 0,
            "total_comments": sum(comments),
            "avg_comments": sum(comments) / len(comments) if comments else 0,
            "date_range": {
                "earliest": min(dates) if dates else None,
                "latest": max(dates) if dates else None
            }
        }
    
    def extract_game_mentions(self, text: str, game_names: list) -> list:
        """
        Extract mentioned game names from text.
        
        Args:
            text: Text to search
            game_names: List of game names to look for
            
        Returns:
            List of mentioned game names
        """
        if not text:
            return []
        
        text_lower = text.lower()
        mentioned = []
        
        for game in game_names:
            if game.lower() in text_lower:
                mentioned.append(game)
        
        return mentioned
    
    def transform_post_for_staging(self, post: dict) -> dict:
        """
        Transform a post into staging table format.
        
        Args:
            post: Post dictionary
            
        Returns:
            Transformed dictionary matching stg_reddit schema
        """
        return {
            "post_id": post.get("post_id"),
            "subreddit": post.get("subreddit"),
            "title": post.get("title"),
            "selftext": post.get("selftext"),
            "score": post.get("score"),
            "num_comments": post.get("num_comments"),
            "created_utc": post.get("created_utc"),
            "author": post.get("author"),
            "data_source": post.get("data_source")
        }
    
    def save_to_json(self, data: list, filepath: str, data_type: str = "posts"):
        """
        Save loaded data to a JSON file.
        
        Args:
            data: List of dictionaries
            filepath: Output file path
            data_type: Type of data ("posts" or "comments")
        """
        # Ensure directory exists
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump({
                "extracted_at": datetime.now().isoformat(),
                "data_type": data_type,
                "count": len(data),
                "data": data
            }, f, indent=2, ensure_ascii=False)
        
        print(f"Saved {len(data)} {data_type} to {filepath}")


# =============================================================================
# Standalone execution for testing
# =============================================================================
if __name__ == "__main__":
    print("\n" + "=" * 50)
    print("  REDDIT LOADER TEST")
    print("=" * 50)
    
    loader = RedditLoader()
    
    # Test 1: Load gaming posts
    print("\n[1/2] Loading gaming posts...")
    try:
        posts = loader.load_gaming_posts()
        
        summary = loader.get_posts_summary(posts)
        print(f"\nPosts Summary:")
        print(f"  Total posts: {summary['count']:,}")
        print(f"  Total score: {summary['total_score']:,}")
        print(f"  Avg score: {summary['avg_score']:.1f}")
        print(f"  Max score: {summary['max_score']:,}")
        print(f"  Total comments: {summary['total_comments']:,}")
        
        print(f"\nSample posts:")
        print("-" * 50)
        for post in posts[:3]:
            title = post['title'][:60] + "..." if len(post['title']) > 60 else post['title']
            print(f"  Title: {title}")
            print(f"  Score: {post['score']}, Comments: {post['num_comments']}")
            print()
        
        # Save to staging
        loader.save_to_json(posts, "data/staging/reddit_posts.json", "posts")
        
    except FileNotFoundError as e:
        print(f"  ✗ {e}")
        print("  Make sure gaming.csv is in data/raw/")
    
    # Test 2: Load sentiment comments
    print("\n[2/2] Loading sentiment comments...")
    try:
        comments = loader.load_sentiment_comments()
        
        print(f"\nSample comments:")
        print("-" * 50)
        for comment in comments[:3]:
            text = comment['text'][:80] + "..." if len(comment['text']) > 80 else comment['text']
            print(f"  Text: {text}")
            print(f"  Sentiment: {comment['sentiment_label']} ({comment['sentiment_score']})")
            print()
        
        # Save to staging
        loader.save_to_json(comments, "data/staging/reddit_comments.json", "comments")
        
    except FileNotFoundError as e:
        print(f"  ✗ {e}")
        print("  Make sure 23k_r_gaming_comments_sentiments.csv is in data/raw/")
    
    print("\n✓ Reddit Loader test complete!")
