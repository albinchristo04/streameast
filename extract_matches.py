import requests
import json
from datetime import datetime, timedelta
import os
import sys
import time

class WatchFootyDataExtractor:
    """
    Extracts match data from WatchFooty API and stores it as JSON.
    API Documentation: https://www.watchfooty.st/en/docs/api
    """
    
    def __init__(self, api_key=None):
        """
        Initialize the extractor with API credentials.
        
        Args:
            api_key: API key for authentication (from GitHub Secrets)
        """
        self.api_key = api_key or os.getenv('WATCHFOOTY_API_KEY')
        # Main API for matches
        self.base_url = 'https://www.watchfooty.st/api/v1'
        # API subdomain for other endpoints
        self.api_subdomain = 'https://api.watchfooty.st/api/v1'
        
        self.headers = {
            'Content-Type': 'application/json',
            'User-Agent': 'WatchFooty-Data-Extractor/1.0'
        }
        if self.api_key:
            self.headers['Authorization'] = f'Bearer {self.api_key}'
        
    def fetch_sports(self):
        """
        Fetch available sports from the API.
        
        Returns:
            dict: Available sports data
        """
        try:
            endpoint = f"{self.api_subdomain}/sports"
            
            print(f"Fetching sports from: {endpoint}")
            
            response = requests.get(
                endpoint,
                headers=self.headers,
                timeout=30
            )
            
            response.raise_for_status()
            data = response.json()
            print(f"✓ Sports fetched successfully")
            return data
            
        except requests.exceptions.RequestException as e:
            print(f"✗ Error fetching sports: {e}")
            if 'response' in locals() and hasattr(response, 'status_code'):
                print(f"  Status code: {response.status_code}")
                print(f"  Response: {response.text[:500]}")
            return None
    
    def fetch_matches(self, sport='football', params=None):
        """
        Fetch matches from the API.
        
        Args:
            sport: Sport type (default: 'football')
            params: Additional query parameters (dict)
            
        Returns:
            dict: Match data from API
        """
        try:
            # WatchFooty API endpoint: /api/v1/matches/{sport}
            endpoint = f"{self.base_url}/matches/{sport}"
            
            query_params = params or {}
                
            print(f"Fetching matches from: {endpoint}")
            if query_params:
                print(f"Parameters: {query_params}")
            
            response = requests.get(
                endpoint,
                headers=self.headers,
                params=query_params,
                timeout=30
            )
            
            response.raise_for_status()
            data = response.json()
            
            # Determine match count
            match_count = 0
            if isinstance(data, list):
                match_count = len(data)
            elif isinstance(data, dict):
                if 'matches' in data:
                    match_count = len(data['matches'])
                elif 'data' in data:
                    match_count = len(data['data'])
            
            print(f"✓ Fetched {match_count} matches")
            return data
            
        except requests.exceptions.RequestException as e:
            print(f"✗ Error fetching matches: {e}")
            if 'response' in locals() and hasattr(response, 'status_code'):
                print(f"  Status code: {response.status_code}")
                print(f"  Response: {response.text[:500]}")
            return None
    
    def fetch_match_details(self, match_id, sport='football'):
        """
        Fetch detailed information for a specific match.
        
        Args:
            match_id: ID of the match
            sport: Sport type (default: 'football')
            
        Returns:
            dict: Detailed match data
        """
        try:
            endpoint = f"{self.base_url}/matches/{sport}/{match_id}"
            
            response = requests.get(
                endpoint,
                headers=self.headers,
                timeout=30
            )
            
            response.raise_for_status()
            return response.json()
            
        except requests.exceptions.RequestException as e:
            print(f"✗ Error fetching match {match_id}: {e}")
            return None
    
    def fetch_leagues(self, sport='football'):
        """
        Fetch available leagues for a sport.
        
        Args:
            sport: Sport type (default: 'football')
        
        Returns:
            dict: Available leagues data
        """
        try:
            # Try multiple possible endpoints
            endpoints = [
                f"{self.api_subdomain}/leagues/{sport}",
                f"{self.base_url}/leagues/{sport}",
                f"{self.api_subdomain}/leagues",
                f"{self.base_url}/leagues"
            ]
            
            for endpoint in endpoints:
                try:
                    print(f"Trying leagues endpoint: {endpoint}")
                    response = requests.get(
                        endpoint,
                        headers=self.headers,
                        timeout=30
                    )
                    
                    if response.status_code == 200:
                        print(f"✓ Leagues fetched successfully")
                        return response.json()
                except:
                    continue
            
            print("✗ Could not fetch leagues from any endpoint")
            return None
            
        except Exception as e:
            print(f"✗ Error fetching leagues: {e}")
            return None
    
    def fetch_all_data(self, sport='football', date_filter=None, league_filter=None):
        """
        Fetch comprehensive match data.
        
        Args:
            sport: Sport type (default: 'football')
            date_filter: Optional date to filter (YYYY-MM-DD)
            league_filter: Optional league to filter
            
        Returns:
            dict: Complete dataset
        """
        all_data = {
            'metadata': {
                'extracted_at': datetime.now().isoformat(),
                'sport': sport,
                'date_filter': date_filter,
                'league_filter': league_filter
            },
            'sports': None,
            'leagues': None,
            'matches': []
        }
        
        # Fetch available sports
        print("\n" + "="*50)
        print("Fetching available sports...")
        print("="*50)
        sports_data = self.fetch_sports()
        if sports_data:
            all_data['sports'] = sports_data
        
        # Small delay to avoid rate limiting
        time.sleep(0.5)
        
        # Fetch leagues
        print("\n" + "="*50)
        print(f"Fetching {sport} leagues...")
        print("="*50)
        leagues_data = self.fetch_leagues(sport=sport)
        if leagues_data:
            all_data['leagues'] = leagues_data
        
        # Small delay to avoid rate limiting
        time.sleep(0.5)
        
        # Fetch matches
        print("\n" + "="*50)
        print(f"Fetching {sport} matches...")
        print("="*50)
        
        params = {}
        if date_filter:
            params['date'] = date_filter
        if league_filter:
            params['league'] = league_filter
        
        matches = self.fetch_matches(sport=sport, params=params if params else None)
        
        if matches:
            # Handle different API response structures
            if isinstance(matches, list):
                all_data['matches'] = matches
            elif isinstance(matches, dict):
                if 'matches' in matches:
                    all_data['matches'] = matches['matches']
                elif 'data' in matches:
                    all_data['matches'] = matches['data']
                else:
                    # Store the entire response if structure is unknown
                    all_data['matches'] = matches
        
        return all_data
    
    def fetch_date_range_data(self, sport='football', days_back=3, days_forward=3):
        """
        Fetch match data for a date range.
        
        Args:
            sport: Sport type
            days_back: Number of days in the past
            days_forward: Number of days in the future
            
        Returns:
            dict: Complete dataset with all matches
        """
        all_data = {
            'metadata': {
                'extracted_at': datetime.now().isoformat(),
                'sport': sport,
                'date_range': {
                    'from': (datetime.now() - timedelta(days=days_back)).strftime('%Y-%m-%d'),
                    'to': (datetime.now() + timedelta(days=days_forward)).strftime('%Y-%m-%d')
                }
            },
            'sports': None,
            'leagues': None,
            'matches_by_date': {}
        }
        
        # Fetch sports and leagues once
        sports_data = self.fetch_sports()
        if sports_data:
            all_data['sports'] = sports_data
        
        time.sleep(0.5)
        
        leagues_data = self.fetch_leagues(sport=sport)
        if leagues_data:
            all_data['leagues'] = leagues_data
        
        time.sleep(0.5)
        
        # Fetch matches for each date
        current_date = datetime.now() - timedelta(days=days_back)
        end_date = datetime.now() + timedelta(days=days_forward)
        
        print("\n" + "="*50)
        print(f"Fetching matches for date range...")
        print("="*50)
        
        while current_date <= end_date:
            date_str = current_date.strftime('%Y-%m-%d')
            print(f"\n📅 Date: {date_str}")
            
            matches = self.fetch_matches(sport=sport, params={'date': date_str})
            
            if matches:
                all_data['matches_by_date'][date_str] = matches
            
            # Rate limiting
            time.sleep(1)
            current_date += timedelta(days=1)
        
        return all_data
    
    def save_to_json(self, data, filename='matches_data.json', output_dir='data'):
        """
        Save data to a JSON file.
        
        Args:
            data: Data to save
            filename: Output filename
            output_dir: Output directory
            
        Returns:
            str: Path to saved file
        """
        try:
            # Create output directory if it doesn't exist
            os.makedirs(output_dir, exist_ok=True)
            
            filepath = os.path.join(output_dir, filename)
            
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            
            # Get file size
            file_size = os.path.getsize(filepath)
            file_size_mb = file_size / (1024 * 1024)
            
            print(f"\n{'='*50}")
            print(f"✓ Data saved successfully!")
            print(f"{'='*50}")
            print(f"📁 File: {filepath}")
            print(f"📊 Size: {file_size_mb:.2f} MB")
            
            # Count matches
            if 'matches' in data:
                if isinstance(data['matches'], list):
                    print(f"⚽ Total matches: {len(data['matches'])}")
            elif 'matches_by_date' in data:
                total = sum(len(m) if isinstance(m, list) else 1 
                           for m in data['matches_by_date'].values())
                print(f"⚽ Total matches: {total}")
                print(f"📅 Dates covered: {len(data['matches_by_date'])}")
            
            return filepath
            
        except Exception as e:
            print(f"\n✗ Error saving data: {e}")
            import traceback
            traceback.print_exc()
            return None


def main():
    """
    Main execution function for GitHub Actions.
    """
    print("\n" + "="*50)
    print("🏆 WatchFooty Match Data Extractor")
    print("="*50)
    
    # Initialize extractor
    extractor = WatchFootyDataExtractor()
    
    # Check if API key is set
    if not extractor.api_key:
        print("\n⚠️  WARNING: No API key found.")
        print("   Set WATCHFOOTY_API_KEY in GitHub Secrets if authentication is required.")
        print("   Continuing without authentication...\n")
    
    # Get configuration from environment variables
    sport = os.getenv('SPORT', 'football')
    mode = os.getenv('FETCH_MODE', 'all')  # 'all', 'date', or 'date_range'
    date_filter = os.getenv('DATE_FILTER')  # YYYY-MM-DD
    league_filter = os.getenv('LEAGUE_FILTER')
    days_back = int(os.getenv('DAYS_BACK', '3'))
    days_forward = int(os.getenv('DAYS_FORWARD', '3'))
    output_file = os.getenv('OUTPUT_FILE', f'{sport}_matches_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json')
    
    print(f"\n📋 Configuration:")
    print(f"   • Sport: {sport}")
    print(f"   • Mode: {mode}")
    print(f"   • Date filter: {date_filter or 'None'}")
    print(f"   • League filter: {league_filter or 'None'}")
    if mode == 'date_range':
        print(f"   • Days back: {days_back}")
        print(f"   • Days forward: {days_forward}")
    print(f"   • Output file: {output_file}")
    print(f"   • Base URL: {extractor.base_url}")
    
    # Fetch data based on mode
    print(f"\n🚀 Starting data extraction...")
    try:
        if mode == 'date_range':
            all_data = extractor.fetch_date_range_data(
                sport=sport,
                days_back=days_back,
                days_forward=days_forward
            )
        else:
            all_data = extractor.fetch_all_data(
                sport=sport,
                date_filter=date_filter,
                league_filter=league_filter
            )
        
        # Save to JSON
        filepath = extractor.save_to_json(all_data, filename=output_file)
        
        if filepath:
            print(f"\n✅ Extraction completed successfully!")
            sys.exit(0)
        else:
            print(f"\n❌ Failed to save data")
            sys.exit(1)
            
    except Exception as e:
        print(f"\n❌ Error during extraction: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
