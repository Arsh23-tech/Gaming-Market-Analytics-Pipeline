"""
Game Matcher
Fuzzy matches game names across different data sources (RAWG, Twitch, Reddit).

Why this is needed:
- RAWG might have "The Witcher 3: Wild Hunt"
- Twitch might have "The Witcher 3"
- Reddit post might mention "Witcher 3" or "TW3"

This module handles these variations to link data across sources.

Usage:
    from transformers.game_matcher import GameMatcher
    
    matcher = GameMatcher()
    matcher.load_games_from_db()
    matches = matcher.match_twitch_to_rawg()
"""

import os
import re
import psycopg2
from typing import Optional, List, Tuple
from collections import defaultdict

# Try to import rapidfuzz (faster) or fall back to fuzzywuzzy
try:
    from rapidfuzz import fuzz, process
    FUZZY_LIB = "rapidfuzz"
except ImportError:
    try:
        from fuzzywuzzy import fuzz, process
        FUZZY_LIB = "fuzzywuzzy"
    except ImportError:
        FUZZY_LIB = None


class GameMatcher:
    """
    Matches game names across different data sources using fuzzy string matching.
    """
    
    # Common words to remove for better matching
    STOP_WORDS = {
        'the', 'a', 'an', 'of', 'and', '&', 'for', 'to', 'in', 'on',
        'game', 'edition', 'definitive', 'complete', 'goty', 'remastered',
        'remake', 'hd', 'ultimate', 'deluxe', 'standard', 'premium'
    }
    
    # Common substitutions
    SUBSTITUTIONS = {
        '&': 'and',
        'ii': '2',
        'iii': '3',
        'iv': '4',
        'v': '5',
        'vi': '6',
        'vii': '7',
        'viii': '8',
        'ix': '9',
        'x': '10',
    }
    
    def __init__(
        self,
        host: Optional[str] = None,
        port: Optional[str] = None,
        dbname: Optional[str] = None,
        user: Optional[str] = None,
        password: Optional[str] = None,
        match_threshold: int = 85
    ):
        """
        Initialize the matcher.
        
        Args:
            host, port, dbname, user, password: Database connection params
            match_threshold: Minimum fuzzy match score (0-100) to consider a match
        """
        if FUZZY_LIB is None:
            raise ImportError(
                "Fuzzy matching library required. Install with:\n"
                "  pip install rapidfuzz\n"
                "or\n"
                "  pip install fuzzywuzzy python-Levenshtein"
            )
        
        self.connection_params = {
            "host": host or os.environ.get("ANALYTICS_DB_HOST", "localhost"),
            "port": port or os.environ.get("ANALYTICS_DB_PORT", "5432"),
            "dbname": dbname or os.environ.get("ANALYTICS_DB_NAME", "gaming_analytics"),
            "user": user or os.environ.get("ANALYTICS_DB_USER", "analytics"),
            "password": password or os.environ.get("ANALYTICS_DB_PASSWORD", "analytics123")
        }
        
        self.match_threshold = match_threshold
        self.conn = None
        self.cursor = None
        
        # Cached game data
        self.rawg_games = []
        self.twitch_games = []
        
        print(f"GameMatcher initialized (using {FUZZY_LIB}, threshold={match_threshold})")
    
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
    
    def normalize_name(self, name: str) -> str:
        """
        Normalize a game name for better matching.
        
        Args:
            name: Original game name
            
        Returns:
            Normalized name
        """
        if not name:
            return ""
        
        # Lowercase
        normalized = name.lower().strip()
        
        # Remove special characters but keep spaces and numbers
        normalized = re.sub(r'[^\w\s]', ' ', normalized)
        
        # Apply substitutions
        words = normalized.split()
        words = [self.SUBSTITUTIONS.get(w, w) for w in words]
        
        # Remove stop words (but keep if it's the only word)
        if len(words) > 1:
            words = [w for w in words if w not in self.STOP_WORDS]
        
        # Rejoin and remove extra spaces
        normalized = ' '.join(words)
        normalized = re.sub(r'\s+', ' ', normalized).strip()
        
        return normalized
    
    def load_games_from_db(self):
        """Load games from staging tables into memory for matching."""
        self.connect()
        
        # Load RAWG games
        print("Loading RAWG games from database...")
        self.cursor.execute("""
            SELECT rawg_id, name, slug FROM stg_rawg WHERE name IS NOT NULL
        """)
        self.rawg_games = [
            {
                "rawg_id": row[0],
                "name": row[1],
                "slug": row[2],
                "normalized": self.normalize_name(row[1])
            }
            for row in self.cursor.fetchall()
        ]
        print(f"  Loaded {len(self.rawg_games)} RAWG games")
        
        # Load Twitch games
        print("Loading Twitch games from database...")
        self.cursor.execute("""
            SELECT DISTINCT twitch_game_id, name FROM stg_twitch WHERE name IS NOT NULL
        """)
        self.twitch_games = [
            {
                "twitch_game_id": row[0],
                "name": row[1],
                "normalized": self.normalize_name(row[1])
            }
            for row in self.cursor.fetchall()
        ]
        print(f"  Loaded {len(self.twitch_games)} Twitch games")
    
    def find_best_match(self, name: str, candidates: List[dict], name_key: str = "normalized") -> Tuple[Optional[dict], int]:
        """
        Find the best matching game from a list of candidates.
        
        Args:
            name: Name to match
            candidates: List of game dictionaries
            name_key: Key in dictionary containing the name to match against
            
        Returns:
            Tuple of (best matching game dict or None, match score)
        """
        if not name or not candidates:
            return None, 0
        
        normalized = self.normalize_name(name)
        
        # Extract names for matching
        candidate_names = [c[name_key] for c in candidates]
        
        # Use rapidfuzz/fuzzywuzzy to find best match
        result = process.extractOne(
            normalized,
            candidate_names,
            scorer=fuzz.token_sort_ratio
        )
        
        if result is None:
            return None, 0
        
        if FUZZY_LIB == "rapidfuzz":
            best_match, score, idx = result
        else:
            best_match, score = result
            idx = candidate_names.index(best_match)
        
        if score >= self.match_threshold:
            return candidates[idx], score
        
        return None, score
    
    def match_twitch_to_rawg(self) -> List[dict]:
        """
        Match Twitch games to RAWG games.
        
        Returns:
            List of match dictionaries with twitch_game_id, rawg_id, and match_score
        """
        print("\nMatching Twitch games to RAWG...")
        
        if not self.rawg_games:
            print("  No RAWG games loaded. Call load_games_from_db() first.")
            return []
        
        matches = []
        matched_count = 0
        
        for twitch_game in self.twitch_games:
            best_match, score = self.find_best_match(
                twitch_game["name"],
                self.rawg_games
            )
            
            match_result = {
                "twitch_game_id": twitch_game["twitch_game_id"],
                "twitch_name": twitch_game["name"],
                "rawg_id": best_match["rawg_id"] if best_match else None,
                "rawg_name": best_match["name"] if best_match else None,
                "match_score": score,
                "matched": best_match is not None
            }
            
            matches.append(match_result)
            
            if best_match:
                matched_count += 1
        
        print(f"  Matched {matched_count}/{len(self.twitch_games)} games ({matched_count/len(self.twitch_games)*100:.1f}%)")
        
        return matches
    
    def extract_game_mentions(self, text: str, min_score: int = 80) -> List[dict]:
        """
        Extract game mentions from text (e.g., Reddit post).
        
        Args:
            text: Text to search for game mentions
            min_score: Minimum match score to consider
            
        Returns:
            List of mentioned games with match scores
        """
        if not text or not self.rawg_games:
            return []
        
        mentions = []
        text_lower = text.lower()
        
        # First, try exact substring matches (faster)
        for game in self.rawg_games:
            game_name_lower = game["name"].lower()
            
            # Skip very short names (too many false positives)
            if len(game_name_lower) < 4:
                continue
            
            if game_name_lower in text_lower:
                mentions.append({
                    "rawg_id": game["rawg_id"],
                    "name": game["name"],
                    "match_type": "exact",
                    "match_score": 100
                })
        
        return mentions
    
    def update_dim_games(self, matches: List[dict]):
        """
        Update dim_games table with Twitch IDs based on matches.
        
        Args:
            matches: List of match dictionaries from match_twitch_to_rawg()
        """
        self.connect()
        
        print("\nUpdating dim_games with Twitch mappings...")
        
        updated = 0
        for match in matches:
            if match["matched"] and match["rawg_id"]:
                self.cursor.execute("""
                    UPDATE dim_games 
                    SET twitch_id = %s, updated_at = CURRENT_TIMESTAMP
                    WHERE rawg_id = %s
                """, (match["twitch_game_id"], match["rawg_id"]))
                
                if self.cursor.rowcount > 0:
                    updated += 1
        
        self.conn.commit()
        print(f"  Updated {updated} games with Twitch IDs")
    
    def populate_dim_games_from_staging(self):
        """
        Populate dim_games from stg_rawg data.
        This moves data from staging to the dimension table.
        """
        self.connect()
        
        print("\nPopulating dim_games from staging...")
        
        # Insert games that don't already exist
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
        
        rows_affected = self.cursor.rowcount
        self.conn.commit()
        
        print(f"  ✓ Inserted/updated {rows_affected} games in dim_games")
        
        return rows_affected
    
    def get_match_report(self, matches: List[dict]) -> str:
        """
        Generate a human-readable match report.
        
        Args:
            matches: List of match dictionaries
            
        Returns:
            Formatted report string
        """
        report = []
        report.append("\n" + "=" * 60)
        report.append("GAME MATCHING REPORT")
        report.append("=" * 60)
        
        matched = [m for m in matches if m["matched"]]
        unmatched = [m for m in matches if not m["matched"]]
        
        report.append(f"\nTotal Twitch games: {len(matches)}")
        report.append(f"Successfully matched: {len(matched)} ({len(matched)/len(matches)*100:.1f}%)")
        report.append(f"Unmatched: {len(unmatched)}")
        
        if matched:
            report.append("\n--- MATCHED GAMES ---")
            for m in sorted(matched, key=lambda x: x["match_score"], reverse=True)[:10]:
                report.append(f"  [{m['match_score']}%] {m['twitch_name']} → {m['rawg_name']}")
        
        if unmatched:
            report.append("\n--- UNMATCHED GAMES ---")
            for m in unmatched[:10]:
                report.append(f"  {m['twitch_name']} (best score: {m['match_score']}%)")
        
        report.append("\n" + "=" * 60)
        
        return "\n".join(report)


# =============================================================================
# Standalone execution for testing
# =============================================================================
if __name__ == "__main__":
    print("\n" + "=" * 50)
    print("  GAME MATCHER TEST")
    print("=" * 50)
    
    # Create matcher
    matcher = GameMatcher(match_threshold=80)
    
    # Test normalization
    print("\n[1/4] Testing name normalization...")
    test_names = [
        "The Witcher 3: Wild Hunt - Game of the Year Edition",
        "ELDEN RING",
        "Grand Theft Auto V",
        "Counter-Strike 2",
        "League of Legends"
    ]
    for name in test_names:
        print(f"  '{name}' → '{matcher.normalize_name(name)}'")
    
    # Load games from database
    print("\n[2/4] Loading games from database...")
    try:
        matcher.load_games_from_db()
    except Exception as e:
        print(f"  ✗ Error: {e}")
        print("  Make sure PostgreSQL is running and has data loaded.")
        exit(1)
    
    # Match Twitch to RAWG
    print("\n[3/4] Matching Twitch games to RAWG...")
    matches = matcher.match_twitch_to_rawg()
    
    # Print report
    print(matcher.get_match_report(matches))
    
    # Populate dim_games
    print("\n[4/4] Populating dim_games table...")
    matcher.populate_dim_games_from_staging()
    
    matcher.disconnect()
    print("\n✓ Game Matcher test complete!")
