"""
RAWG API Extractor
Fetches game data from RAWG.io API with pagination, rate limiting, and error handling.

Usage:
    from extractors.rawg_extractor import RAWGExtractor
    
    extractor = RAWGExtractor(api_key="your_key")
    games = extractor.extract_games(page_size=40, max_pages=5)
"""

import os
import json
import time
import requests
from datetime import datetime, timedelta
from typing import Optional


class RAWGExtractor:
    """
    Extracts game data from the RAWG API.
    
    RAWG API Documentation: https://api.rawg.io/docs/
    Free tier: 20,000 requests/month
    """
    
    BASE_URL = "https://api.rawg.io/api"
    
    def __init__(self, api_key: Optional[str] = None):
        """
        Initialize the extractor.
        
        Args:
            api_key: RAWG API key. If not provided, reads from RAWG_API_KEY env variable.
        """
        self.api_key = api_key or os.environ.get("RAWG_API_KEY")
        
        if not self.api_key:
            raise ValueError("RAWG API key is required. Set RAWG_API_KEY environment variable or pass api_key parameter.")
        
        self.session = requests.Session()
        self.request_count = 0
        self.last_request_time = None
    
    def _rate_limit(self):
        """
        Implement rate limiting to avoid hitting API limits.
        RAWG doesn't specify strict rate limits, but we'll be respectful.
        """
        if self.last_request_time:
            elapsed = time.time() - self.last_request_time
            if elapsed < 0.25:  # Max ~4 requests per second
                time.sleep(0.25 - elapsed)
        
        self.last_request_time = time.time()
    
    def _make_request(self, endpoint: str, params: dict = None) -> dict:
        """
        Make a request to the RAWG API.
        
        Args:
            endpoint: API endpoint (e.g., "/games")
            params: Query parameters
            
        Returns:
            JSON response as dictionary
        """
        self._rate_limit()
        
        url = f"{self.BASE_URL}{endpoint}"
        
        # Always include API key
        if params is None:
            params = {}
        params["key"] = self.api_key
        
        try:
            response = self.session.get(url, params=params, timeout=30)
            self.request_count += 1
            
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 401:
                raise ValueError("Invalid API key")
            elif response.status_code == 429:
                print("Rate limited. Waiting 60 seconds...")
                time.sleep(60)
                return self._make_request(endpoint, params)
            else:
                print(f"API error {response.status_code}: {response.text[:200]}")
                return None
                
        except requests.exceptions.Timeout:
            print(f"Request timeout for {endpoint}")
            return None
        except requests.exceptions.RequestException as e:
            print(f"Request error: {e}")
            return None
    
    def extract_games(
        self,
        page_size: int = 40,
        max_pages: int = 10,
        dates_from: Optional[str] = None,
        dates_to: Optional[str] = None,
        metacritic_min: Optional[int] = None,
        ordering: str = "-rating",
        genres: Optional[str] = None,
        tags: Optional[str] = None
    ) -> list:
        """
        Extract games from RAWG API with pagination.
        
        Args:
            page_size: Number of games per page (max 40)
            max_pages: Maximum number of pages to fetch
            dates_from: Filter games released after this date (YYYY-MM-DD)
            dates_to: Filter games released before this date (YYYY-MM-DD)
            metacritic_min: Minimum Metacritic score
            ordering: Sort order (-rating, -released, -metacritic, etc.)
            genres: Filter by genre slugs (comma-separated)
            tags: Filter by tag slugs (comma-separated)
            
        Returns:
            List of game dictionaries
        """
        all_games = []
        
        # Build date range if not specified (default: last 2 years)
        if not dates_from:
            two_years_ago = datetime.now() - timedelta(days=730)
            dates_from = two_years_ago.strftime("%Y-%m-%d")
        
        if not dates_to:
            dates_to = datetime.now().strftime("%Y-%m-%d")
        
        params = {
            "page_size": min(page_size, 40),  # API max is 40
            "dates": f"{dates_from},{dates_to}",
            "ordering": ordering
        }
        
        if metacritic_min:
            params["metacritic"] = f"{metacritic_min},100"
        
        if genres:
            params["genres"] = genres
            
        if tags:
            params["tags"] = tags
        
        print(f"Extracting games from RAWG API...")
        print(f"  Date range: {dates_from} to {dates_to}")
        print(f"  Page size: {page_size}, Max pages: {max_pages}")
        
        for page in range(1, max_pages + 1):
            params["page"] = page
            
            print(f"  Fetching page {page}/{max_pages}...", end=" ")
            
            data = self._make_request("/games", params)
            
            if not data or "results" not in data:
                print("No data returned")
                break
            
            games = data["results"]
            all_games.extend(games)
            
            print(f"Got {len(games)} games (Total: {len(all_games)})")
            
            # Check if there are more pages
            if not data.get("next"):
                print("  No more pages available")
                break
        
        print(f"\nExtraction complete!")
        print(f"  Total games extracted: {len(all_games)}")
        print(f"  API requests made: {self.request_count}")
        
        return all_games
    
    def extract_game_details(self, game_id: int) -> dict:
        """
        Extract detailed information for a specific game.
        
        Args:
            game_id: RAWG game ID
            
        Returns:
            Game details dictionary
        """
        return self._make_request(f"/games/{game_id}")
    
    def extract_genres(self) -> list:
        """
        Extract all available genres from RAWG.
        
        Returns:
            List of genre dictionaries
        """
        data = self._make_request("/genres", {"page_size": 50})
        return data.get("results", []) if data else []
    
    def extract_platforms(self) -> list:
        """
        Extract all available platforms from RAWG.
        
        Returns:
            List of platform dictionaries
        """
        all_platforms = []
        page = 1
        
        while True:
            data = self._make_request("/platforms", {"page_size": 50, "page": page})
            
            if not data or "results" not in data:
                break
            
            all_platforms.extend(data["results"])
            
            if not data.get("next"):
                break
            
            page += 1
        
        return all_platforms
    
    def transform_game_for_staging(self, game: dict) -> dict:
        """
        Transform a raw RAWG game object into our staging table format.
        
        Args:
            game: Raw game dictionary from RAWG API
            
        Returns:
            Transformed dictionary matching stg_rawg schema
        """
        return {
            "rawg_id": game.get("id"),
            "name": game.get("name"),
            "slug": game.get("slug"),
            "released": game.get("released"),
            "metacritic": game.get("metacritic"),
            "rating": game.get("rating"),
            "playtime": game.get("playtime"),
            "genres": json.dumps(game.get("genres", [])),
            "tags": json.dumps(game.get("tags", [])),
            "platforms": json.dumps(game.get("platforms", []))
        }
    
    def save_to_json(self, games: list, filepath: str):
        """
        Save extracted games to a JSON file.
        
        Args:
            games: List of game dictionaries
            filepath: Output file path
        """
        # Ensure directory exists
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        
        # Transform games for staging
        transformed = [self.transform_game_for_staging(g) for g in games]
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump({
                "extracted_at": datetime.now().isoformat(),
                "count": len(transformed),
                "games": transformed
            }, f, indent=2, ensure_ascii=False)
        
        print(f"Saved {len(transformed)} games to {filepath}")


# =============================================================================
# Standalone execution for testing
# =============================================================================
if __name__ == "__main__":
    import sys
    
    # Check for API key
    api_key = os.environ.get("RAWG_API_KEY")
    
    if not api_key:
        print("Please set RAWG_API_KEY environment variable")
        print("  Windows: set RAWG_API_KEY=your_key_here")
        print("  Mac/Linux: export RAWG_API_KEY=your_key_here")
        sys.exit(1)
    
    # Create extractor
    extractor = RAWGExtractor(api_key)
    
    # Test extraction (small sample)
    print("\n" + "=" * 50)
    print("  RAWG EXTRACTOR TEST")
    print("=" * 50)
    
    # Extract a small sample of games
    games = extractor.extract_games(
        page_size=10,
        max_pages=2,
        metacritic_min=70
    )
    
    # Show sample
    print("\nSample games extracted:")
    print("-" * 50)
    for game in games[:5]:
        print(f"  {game['name']}")
        print(f"    Released: {game.get('released', 'N/A')}")
        print(f"    Metacritic: {game.get('metacritic', 'N/A')}")
        print(f"    Rating: {game.get('rating', 'N/A')}")
        genres = [g['name'] for g in game.get('genres', [])]
        print(f"    Genres: {', '.join(genres) if genres else 'N/A'}")
        print()
    
    # Save to file
    output_path = "data/staging/rawg_games_test.json"
    extractor.save_to_json(games, output_path)
    
    print("\n✓ RAWG Extractor test complete!")
