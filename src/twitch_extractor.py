"""
Twitch API Extractor
Fetches top games and streaming data from Twitch Helix API with OAuth authentication.

Usage:
    from extractors.twitch_extractor import TwitchExtractor
    
    extractor = TwitchExtractor(client_id="your_id", client_secret="your_secret")
    top_games = extractor.extract_top_games(limit=100)
"""

import os
import json
import time
import requests
from datetime import datetime
from typing import Optional


class TwitchExtractor:
    """
    Extracts streaming data from the Twitch Helix API.
    
    Twitch API Documentation: https://dev.twitch.tv/docs/api/reference
    Requires OAuth Client Credentials flow for app access.
    """
    
    AUTH_URL = "https://id.twitch.tv/oauth2/token"
    BASE_URL = "https://api.twitch.tv/helix"
    
    # Non-game categories to exclude from analysis
    EXCLUDED_CATEGORIES = {
        "just chatting",
        "music",
        "art",
        "sports",
        "talk shows & podcasts",
        "asmr",
        "pools, hot tubs, and beaches",
        "special events",
        "travel & outdoors",
        "makers & crafting",
        "food & drink",
        "i'm only sleeping",
        "fitness & health",
        "beauty & body art",
        "science & technology",
        "poker",
        "chess",
        "casino",
        "slots",
        "virtual casino",
        "politics",
        "software and game development",
        "animals, aquariums, and zoos",
    }
    
    def __init__(
        self,
        client_id: Optional[str] = None,
        client_secret: Optional[str] = None
    ):
        """
        Initialize the extractor and obtain OAuth token.
        
        Args:
            client_id: Twitch Client ID. If not provided, reads from TWITCH_CLIENT_ID env variable.
            client_secret: Twitch Client Secret. If not provided, reads from TWITCH_CLIENT_SECRET env variable.
        """
        self.client_id = client_id or os.environ.get("TWITCH_CLIENT_ID")
        self.client_secret = client_secret or os.environ.get("TWITCH_CLIENT_SECRET")
        
        if not self.client_id or not self.client_secret:
            raise ValueError(
                "Twitch credentials required. Set TWITCH_CLIENT_ID and TWITCH_CLIENT_SECRET "
                "environment variables or pass them as parameters."
            )
        
        self.session = requests.Session()
        self.access_token = None
        self.token_expires_at = None
        self.request_count = 0
        self.last_request_time = None
        
        # Get initial token
        self._authenticate()
    
    def _authenticate(self):
        """
        Obtain OAuth access token using Client Credentials flow.
        """
        print("Authenticating with Twitch...")
        
        response = requests.post(
            self.AUTH_URL,
            params={
                "client_id": self.client_id,
                "client_secret": self.client_secret,
                "grant_type": "client_credentials"
            },
            timeout=10
        )
        
        if response.status_code != 200:
            raise ValueError(f"Authentication failed: {response.text}")
        
        data = response.json()
        self.access_token = data["access_token"]
        # Token typically expires in ~60 days, but we'll refresh proactively
        self.token_expires_at = datetime.now().timestamp() + data.get("expires_in", 3600)
        
        print("  ✓ Authentication successful!")
    
    def _ensure_valid_token(self):
        """
        Check if token is valid and refresh if needed.
        """
        if not self.access_token or datetime.now().timestamp() >= self.token_expires_at - 60:
            self._authenticate()
    
    def _rate_limit(self):
        """
        Implement rate limiting.
        Twitch allows 800 requests per minute for most endpoints.
        """
        if self.last_request_time:
            elapsed = time.time() - self.last_request_time
            if elapsed < 0.1:  # Max ~10 requests per second to be safe
                time.sleep(0.1 - elapsed)
        
        self.last_request_time = time.time()
    
    def _make_request(self, endpoint: str, params: dict = None) -> dict:
        """
        Make an authenticated request to the Twitch API.
        
        Args:
            endpoint: API endpoint (e.g., "/games/top")
            params: Query parameters
            
        Returns:
            JSON response as dictionary
        """
        self._ensure_valid_token()
        self._rate_limit()
        
        url = f"{self.BASE_URL}{endpoint}"
        
        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Client-Id": self.client_id
        }
        
        try:
            response = self.session.get(url, headers=headers, params=params, timeout=30)
            self.request_count += 1
            
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 401:
                # Token expired, refresh and retry
                print("Token expired, refreshing...")
                self._authenticate()
                return self._make_request(endpoint, params)
            elif response.status_code == 429:
                # Rate limited
                retry_after = int(response.headers.get("Retry-After", 60))
                print(f"Rate limited. Waiting {retry_after} seconds...")
                time.sleep(retry_after)
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
    
    def extract_top_games(self, limit: int = 100, include_non_games: bool = False) -> list:
        """
        Extract top games by current viewership.
        
        Args:
            limit: Number of top games to fetch (max 100 per request)
            include_non_games: If False, filters out non-game categories like "Just Chatting"
            
        Returns:
            List of game dictionaries with viewer counts
        """
        print(f"Extracting top {limit} games from Twitch...")
        if not include_non_games:
            print("  (Excluding non-game categories like 'Just Chatting')")
        
        all_games = []
        cursor = None
        
        # Fetch extra to account for filtered items
        fetch_limit = limit * 2 if not include_non_games else limit
        
        while len(all_games) < limit:
            # Twitch max is 100 per request
            fetch_count = min(100, fetch_limit - len(all_games))
            
            params = {"first": fetch_count}
            if cursor:
                params["after"] = cursor
            
            data = self._make_request("/games/top", params)
            
            if not data or "data" not in data:
                break
            
            games = data["data"]
            
            # Filter out non-game categories if requested
            if not include_non_games:
                games = [
                    g for g in games 
                    if g.get("name", "").strip().lower() not in self.EXCLUDED_CATEGORIES
                ]
                # Debug: show what's being filtered
                for g in data["data"]:
                    if g.get("name", "").strip().lower() in self.EXCLUDED_CATEGORIES:
                        print(f"    Filtered out: {g.get('name')}")
            
            all_games.extend(games)
            
            print(f"  Fetched batch (Total valid games: {len(all_games)})")
            
            # Check for pagination
            cursor = data.get("pagination", {}).get("cursor")
            if not cursor:
                break
            
            # Stop if we have enough
            if len(all_games) >= limit:
                all_games = all_games[:limit]
                break
        
        # Now get viewer counts for each game by checking streams
        print("  Fetching viewer counts...")
        games_with_viewers = self._enrich_with_viewer_counts(all_games)
        
        print(f"\nExtraction complete!")
        print(f"  Total games extracted: {len(games_with_viewers)}")
        print(f"  API requests made: {self.request_count}")
        
        return games_with_viewers
    
    def _enrich_with_viewer_counts(self, games: list) -> list:
        """
        Add viewer counts and stream counts to game data.
        
        Args:
            games: List of game dictionaries from /games/top
            
        Returns:
            Games enriched with viewer_count and stream_count
        """
        enriched = []
        
        for i, game in enumerate(games):
            game_id = game["id"]
            
            # Get streams for this game
            streams_data = self._make_request("/streams", {
                "game_id": game_id,
                "first": 100  # Get top 100 streams to calculate totals
            })
            
            if streams_data and "data" in streams_data:
                streams = streams_data["data"]
                total_viewers = sum(s.get("viewer_count", 0) for s in streams)
                stream_count = len(streams)
            else:
                total_viewers = 0
                stream_count = 0
            
            enriched.append({
                **game,
                "viewer_count": total_viewers,
                "stream_count": stream_count,
                "rank_position": i + 1
            })
            
            # Progress indicator for larger fetches
            if (i + 1) % 20 == 0:
                print(f"    Processed {i + 1}/{len(games)} games...")
        
        return enriched
    
    def extract_streams_for_game(self, game_id: str, limit: int = 100) -> list:
        """
        Extract active streams for a specific game.
        
        Args:
            game_id: Twitch game ID
            limit: Maximum number of streams to fetch
            
        Returns:
            List of stream dictionaries
        """
        all_streams = []
        cursor = None
        
        while len(all_streams) < limit:
            fetch_count = min(100, limit - len(all_streams))
            
            params = {
                "game_id": game_id,
                "first": fetch_count
            }
            if cursor:
                params["after"] = cursor
            
            data = self._make_request("/streams", params)
            
            if not data or "data" not in data:
                break
            
            streams = data["data"]
            all_streams.extend(streams)
            
            cursor = data.get("pagination", {}).get("cursor")
            if not cursor or not streams:
                break
        
        return all_streams
    
    def search_games(self, query: str, limit: int = 20) -> list:
        """
        Search for games by name.
        
        Args:
            query: Search query
            limit: Maximum results
            
        Returns:
            List of matching games
        """
        data = self._make_request("/search/categories", {
            "query": query,
            "first": min(limit, 100)
        })
        
        return data.get("data", []) if data else []
    
    def transform_game_for_staging(self, game: dict) -> dict:
        """
        Transform a Twitch game object into our staging table format.
        
        Args:
            game: Game dictionary from Twitch API
            
        Returns:
            Transformed dictionary matching stg_twitch schema
        """
        return {
            "twitch_game_id": game.get("id"),
            "name": game.get("name"),
            "box_art_url": game.get("box_art_url", "").replace("{width}", "285").replace("{height}", "380"),
            "viewer_count": game.get("viewer_count", 0),
            "stream_count": game.get("stream_count", 0),
            "rank_position": game.get("rank_position"),
            "snapshot_time": datetime.now().isoformat()
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
                "snapshot_time": datetime.now().isoformat(),
                "count": len(transformed),
                "games": transformed
            }, f, indent=2, ensure_ascii=False)
        
        print(f"Saved {len(transformed)} games to {filepath}")


# =============================================================================
# Standalone execution for testing
# =============================================================================
if __name__ == "__main__":
    import sys
    
    # Check for credentials
    client_id = os.environ.get("TWITCH_CLIENT_ID")
    client_secret = os.environ.get("TWITCH_CLIENT_SECRET")
    
    if not client_id or not client_secret:
        print("Please set Twitch credentials as environment variables:")
        print("  Windows:")
        print("    set TWITCH_CLIENT_ID=your_client_id")
        print("    set TWITCH_CLIENT_SECRET=your_client_secret")
        print("  Mac/Linux:")
        print("    export TWITCH_CLIENT_ID=your_client_id")
        print("    export TWITCH_CLIENT_SECRET=your_client_secret")
        sys.exit(1)
    
    # Create extractor
    extractor = TwitchExtractor(client_id, client_secret)
    
    # Test extraction
    print("\n" + "=" * 50)
    print("  TWITCH EXTRACTOR TEST")
    print("=" * 50)
    
    # Extract top 10 games (small test)
    games = extractor.extract_top_games(limit=10)
    
    # Show results
    print("\nTop 10 games on Twitch right now:")
    print("-" * 50)
    for game in games:
        print(f"  #{game['rank_position']}: {game['name']}")
        print(f"      Viewers: {game['viewer_count']:,}")
        print(f"      Streams: {game['stream_count']}")
        print()
    
    # Save to file
    output_path = "data/staging/twitch_top_games_test.json"
    extractor.save_to_json(games, output_path)
    
    print("\n✓ Twitch Extractor test complete!")
