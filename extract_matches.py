import requests
import json
from datetime import datetime, timedelta
import os
import sys

class WatchFootyDataExtractor:
    """
    Extracts match data from WatchFooty API and stores it as JSON.
    Configure the API_KEY and BASE_URL according to WatchFooty API documentation.
    """
    
    def __init__(self, api_key=None, base_url=None):
        """
        Initialize the extractor with API credentials.
        
        Args:
            api_key: API key for authentication (from GitHub Secrets)
            base_url: Base URL for the WatchFooty API
        """
        self.api_key = api_key or os.getenv('WATCHFOOTY_API_KEY')
        self.base_url = base_url or os.getenv('WATCHFOOTY_API_URL', 'https://www.watchfooty.st/api/v1')
        self.headers = {
            'Content-Type': 'application/json',
        }
        if self.api_key:
            self.headers['Authorization'] = f'Bearer {self.api_key}'
        
    def fetch_matches(self, sport='football', date=None, league=None, status=None):
        """
        Fetch matches from the API.
        
        Args:
            sport: Sport type (default: 'football')
            date: Date to fetch matches for (format: YYYY-MM-DD)
            league: League ID or name to filter by
            status: Match status (live, scheduled, finished)
            
        Returns:
            dict: Match data from API
        """
        try:
            # WatchFooty API endpoint structure: /api/v1/matches/{sport}
            endpoint = f"{self.base_url}/matches/{sport}"
            
            params = {}
            if date:
                params['date'] = date
            if league:
                params['league'] = league
            if status:
                params['status'] = status
                
            print(f"Fetching matches from: {endpoint}")
            print(f"Parameters: {params}")
            
            response = requests.get(
                endpoint,
                headers=self.headers,
                params=params,
                timeout=30
            )
            
            response.raise_for_status()
            return response.json()
            
        except requests.exceptions.RequestException as e:
            print(f"Error fetching matches: {e}")
            print(f"Response status: {response.status_code if 'response' in locals() else 'N/A'}")
            if 'response' in locals():
                print(f"Response body: {response.text[:500]}")
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
            print(f"Error fetching match {match_id}: {e}")
            return None
    
    def fetch_leagues(self, sport='football'):
        """
        Fetch available leagues.
        
        Args:
            sport: Sport type (default: 'football')
        
        Returns:
            dict: Available leagues data
        """
        try:
            endpoint = f"{self.base_url}/leagues/{sport}"
            
            response = requests.get(
                endpoint,
                headers=self.headers,
                timeout=30
            )
            
            response.raise_for_status()
            return response.json()
            
        except requests.exceptions.RequestException as e:
            print(f"Error fetching leagues: {e}")
            return None
    
    def fetch_all_data(self, days_range=7, sport='football'):
        """
        Fetch comprehensive match data for a date range.
        
        Args:
            days_range: Number of days to fetch (past and future)
            sport: Sport type (default: 'football')
            
        Returns:
            dict: Complete dataset
        """
        all_data = {
            'metadata': {
                'extracted_at': datetime.now().isoformat(),
                'sport': sport,
                'date_range': {
                    'from': (datetime.now() - timedelta(days=days_range)).strftime('%Y-%m-%d'),
                    'to': (datetime.now() + timedelta(days=days_range)).strftime('%Y-%m-%d')
                }
            },
            'leagues': [],
            'matches': []
        }
        
        # Fetch leagues
        print(f"Fetching {sport} leagues...")
        leagues_data = self.fetch_leagues(sport=sport)
        if leagues_data:
            all_data['leagues'] = leagues_data
        
        # Fetch matches for date range
        current_date = datetime.now() - timedelta(days=days_range)
        end_date = datetime.now() + timedelta(days=days_range)
        
        while current_date <= end_date:
            date_str = current_date.strftime('%Y-%m-%d')
            print(f"Fetching matches for {date_str}...")
            
            matches = self.fetch_matches(sport=sport, date=date_str)
            if matches:
                # If API returns a list directly
                if isinstance(matches, list):
                    all_data['matches'].extend(matches)
                # If API returns a dict with matches key
                elif isinstance(matches, dict) and 'matches' in matches:
                    all_data['matches'].extend(matches['matches'])
                elif isinstance(matches, dict) and 'data' in matches:
                    all_data['matches'].extend(matches['data'])
                # Otherwise store the entire response
                else:
                    all_data['matches'].append({
                        'date': date_str,
                        'data': matches
                    })
            
            current_date += timedelta(days=1)
        
        return all_data
    
    def save_to_json(self, data, filename='matches_data.json', output_dir='data'):
        """
        Save data to a JSON file.
        
        Args:
            data: Data to save
            filename: Output filename
            output_dir: Output directory
        """
        try:
            # Create output directory if it doesn't exist
            os.makedirs(output_dir, exist_ok=True)
            
            filepath = os.path.join(output_dir, filename)
            
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            
            print(f"Data saved successfully to {filepath}")
            print(f"Total matches: {len(data.get('matches', []))}")
            
            return filepath
            
        except Exception as e:
            print(f"Error saving data: {e}")
            return None


def main():
    """
    Main execution function for GitHub Actions.
    """
    print("=" * 50)
    print("WatchFooty Match Data Extractor")
    print("=" * 50)
    
    # Initialize extractor
    extractor = WatchFootyDataExtractor()
    
    # Check if API key is set
    if not extractor.api_key:
        print("WARNING: No API key found. Set WATCHFOOTY_API_KEY in GitHub Secrets.")
        print("Continuing with potential limited access...")
    
    # Get configuration from environment variables
    days_range = int(os.getenv('DAYS_RANGE', '7'))
    output_file = os.getenv('OUTPUT_FILE', f'matches_{datetime.now().strftime("%Y%m%d")}.json')
    
    print(f"\nConfiguration:")
    print(f"- Days range: {days_range}")
    print(f"- Output file: {output_file}")
    print(f"- Base URL: {extractor.base_url}")
    
    # Fetch all data
    print("\nStarting data extraction...")
    try:
        all_data = extractor.fetch_all_data(days_range=days_range)
        
        # Save to JSON
        filepath = extractor.save_to_json(all_data, filename=output_file)
        
        if filepath:
            print(f"\n✓ Successfully extracted and saved match data!")
            print(f"✓ File: {filepath}")
            sys.exit(0)
        else:
            print("\n✗ Failed to save data")
            sys.exit(1)
            
    except Exception as e:
        print(f"\n✗ Error during extraction: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
