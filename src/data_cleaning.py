import pandas as pd
import numpy as np
import os

def clean_mlb_data():
    """Simple but effective data cleaning for MLB data"""
    
    print("Starting simple data cleaning...")
    
    # Load data
    try:
        hitting_df = pd.read_csv('data/raw/yearly_hitting_leaders.csv')
        pitching_df = pd.read_csv('data/raw/yearly_pitching_leaders.csv')
        standings_df = pd.read_csv('data/raw/team_standings.csv')
        events_df = pd.read_csv('data/raw/notable_events.csv')
    except FileNotFoundError as e:
        print(f"Error loading data: {e}")
        return
    
    print(f"Loaded: {len(hitting_df)} hitting, {len(pitching_df)} pitching, "
          f"{len(standings_df)} standings, {len(events_df)} events")
    
    # Team name mapping from city to full team name
    team_mapping = {
        'New York': 'New York Yankees',
        'Boston': 'Boston Red Sox',
        'Detroit': 'Detroit Tigers', 
        'Chicago': 'Chicago White Sox',  # Default to AL team
        'Philadelphia': 'Philadelphia Athletics',
        'Washington': 'Washington Senators',
        'St. Louis': 'St. Louis Cardinals',
        'Cleveland': 'Cleveland Indians',
        'Baltimore': 'Baltimore Orioles',
        'Minnesota': 'Minnesota Twins',
        'Oakland': 'Oakland Athletics',
        'Kansas City': 'Kansas City Royals',
        'Milwaukee': 'Milwaukee Brewers',
        'Toronto': 'Toronto Blue Jays',
        'Seattle': 'Seattle Mariners',
        'Tampa Bay': 'Tampa Bay Rays',
        'Los Angeles': 'Los Angeles Angels',
        'Anaheim': 'Los Angeles Angels',
        'California': 'Los Angeles Angels',
        'Texas': 'Texas Rangers',
        'Houston': 'Houston Astros'
    }
    
    def standardize_team_name(team, year):
        """Standardize team names based on city and year"""
        if pd.isna(team):
            return 'Unknown'
        
        team = str(team).strip()
        
        # Handle special cases based on year
        if team == 'Chicago':
            # Could be Cubs or White Sox - assume White Sox for AL context
            return 'Chicago White Sox'
        elif team == 'St. Louis':
            if year <= 1953:
                return 'St. Louis Browns'
            else:
                return 'St. Louis Cardinals'
        elif team == 'Washington':
            if year <= 1971:
                return 'Washington Senators'
            else:
                return 'Washington Nationals'
        elif team in ['Los Angeles', 'California', 'Anaheim']:
            return 'Los Angeles Angels'
        
        # Use mapping for other teams
        return team_mapping.get(team, team)
    
    # Clean hitting data
    print("Cleaning hitting data...")
    hitting_df = hitting_df.dropna(subset=['player_name', 'stat_value'])
    hitting_df['player_name'] = hitting_df['player_name'].str.strip()
    hitting_df['team'] = hitting_df['team'].fillna('Unknown')
    
    # Standardize team names in hitting data
    hitting_df['team'] = hitting_df.apply(lambda row: standardize_team_name(row['team'], row['year']), axis=1)
    
    # Validate ranges by category (updated with more relevant stats)
    stat_ranges = {
        'Home Runs': (0, 100),
        'Batting Average': (0.200, 0.500),  # More realistic minimum
        'RBI': (50, 200),  # Leaders typically 50+
        'Runs': (50, 200),
        'Hits': (100, 300),
        'Doubles': (20, 70),
        'Triples': (5, 30),
        'On Base Percentage': (0.300, 0.600),
        'Slugging Average': (0.400, 0.900),
        'Base on Balls': (50, 200),
        'Total Bases': (200, 500),
        # Pitching stats
        'ERA': (1.00, 6.00),  # Leaders typically under 6
        'Wins': (10, 35),
        'Strikeouts': (100, 400),
        'Saves': (20, 70),  # Modern save totals
        'Complete Games': (5, 40),
        'Shutouts': (2, 15),
        'Winning Percentage': (0.500, 1.000)  # Leaders above .500
    }
    
    # Remove obviously bad data with improved validation
    def is_valid_stat(row):
        category = row.get('stat_category', '')
        value = row.get('stat_value', 0)
        
        if category in stat_ranges:
            min_val, max_val = stat_ranges[category]
            return min_val <= value <= max_val
        
        # For unknown categories, just check if positive
        return value >= 0
    
    hitting_df = hitting_df[hitting_df.apply(is_valid_stat, axis=1)]
    pitching_df = pitching_df[pitching_df.apply(is_valid_stat, axis=1)]
    
    # Clean pitching data
    print("Cleaning pitching data...")
    pitching_df = pitching_df.dropna(subset=['player_name', 'stat_value'])
    pitching_df['player_name'] = pitching_df['player_name'].str.strip()
    pitching_df['team'] = pitching_df['team'].fillna('Unknown')
    
    # Standardize team names in pitching data
    pitching_df['team'] = pitching_df.apply(lambda row: standardize_team_name(row['team'], row['year']), axis=1)
    
    # Remove obviously bad data with improved validation
    pitching_df = pitching_df[pitching_df.apply(is_valid_stat, axis=1)]
    pitching_df = pitching_df[pitching_df['player_name'].str.len() > 2]
    
    # Clean standings data
    print("Cleaning standings data...")
    standings_df = standings_df.dropna(subset=['team_name', 'wins', 'losses'])
    standings_df = standings_df[(standings_df['wins'] >= 30) & (standings_df['wins'] <= 130)]
    standings_df = standings_df[(standings_df['losses'] >= 30) & (standings_df['losses'] <= 130)]
    
    # Recalculate win percentage
    standings_df['win_pct'] = standings_df['wins'] / (standings_df['wins'] + standings_df['losses'])
    standings_df['win_pct'] = standings_df['win_pct'].round(3)
    
    # Clean events data
    print("Cleaning events data...")
    events_df = events_df.dropna(subset=['description'])
    events_df['description'] = events_df['description'].str.strip()
    
    # Remove very short descriptions
    events_df = events_df[events_df['description'].str.len() >= 30]
    
    # Improve event classification with more specific categories
    def reclassify_event(description):
        desc_lower = description.lower()
        
        # Most specific first
        if any(term in desc_lower for term in ['world series', 'championship', 'swept']):
            return 'Championships'
        elif any(term in desc_lower for term in ['no-hitter', 'no-hit', 'perfect game']):
            return 'Pitching Feats'
        elif any(term in desc_lower for term in ['record', 'first player', 'first time', 'broke', 'set a new', 'milestone']):
            return 'Records Broken'
        elif any(term in desc_lower for term in ['debut', 'first game', 'rookie', 'first african-american', 'first black']):
            return 'Player Debuts'
        elif any(term in desc_lower for term in ['retire', 'retirement', 'final game', 'last season']):
            return 'Career Endings'
        elif any(term in desc_lower for term in ['death', 'died', 'passed away']):
            return 'Deaths'
        elif any(term in desc_lower for term in ['mvp', 'cy young', 'hall of fame', 'award', 'honor']):
            return 'Awards & Honors'
        elif any(term in desc_lower for term in ['trade', 'traded', 'signed', 'contract', 'acquired']):
            return 'Trades & Signings'
        elif any(term in desc_lower for term in ['strike', 'lockout', 'union', 'players association', 'salary']):
            return 'Labor Issues'
        elif any(term in desc_lower for term in ['rule', 'designated hitter', 'mound', 'expansion', 'playoff']):
            return 'Rule Changes'
        elif any(term in desc_lower for term in ['stadium', 'ballpark', 'field', 'opening day']):
            return 'Stadium Events'
        elif any(term in desc_lower for term in ['injury', 'injured', 'hospital', 'surgery']):
            return 'Injuries'
        elif any(term in desc_lower for term in ['celebration', 'ceremony', 'day', 'honor', 'tribute']):
            return 'Ceremonies'
        elif any(term in desc_lower for term in ['season', 'games', 'schedule', 'postponed', 'cancelled']):
            return 'Season Events'
        else:
            # More specific fallback based on content
            if 'home run' in desc_lower or 'homer' in desc_lower:
                return 'Home Run Events'
            elif any(team in desc_lower for team in ['yankees', 'red sox', 'cubs', 'dodgers']):
                return 'Team Milestones'
            elif any(term in desc_lower for term in ['game', 'inning', 'hit', 'run', 'win']):
                return 'Game Highlights'
            else:
                return 'Historical Notes'
    
    events_df['event_type'] = events_df['description'].apply(reclassify_event)
    
    # Create output directory
    os.makedirs('data/cleaned', exist_ok=True)
    
    # Save cleaned data
    hitting_df.to_csv('data/cleaned/yearly_hitting_leaders_cleaned.csv', index=False)
    pitching_df.to_csv('data/cleaned/yearly_pitching_leaders_cleaned.csv', index=False)
    standings_df.to_csv('data/cleaned/team_standings_cleaned.csv', index=False)
    events_df.to_csv('data/cleaned/notable_events_cleaned.csv', index=False)
    
    print(f"\nCleaned data saved!")
    print(f"Final counts: {len(hitting_df)} hitting, {len(pitching_df)} pitching, "
          f"{len(standings_df)} standings, {len(events_df)} events")
    
    # Quick summary
    print(f"\nQuick Summary:")
    print(f"Years covered: {sorted(standings_df['year'].unique())}")
    print(f"Teams: {standings_df['team_name'].nunique()}")
    print(f"Hitting categories: {hitting_df['stat_category'].nunique()}")
    print(f"Pitching categories: {pitching_df['stat_category'].nunique()}")
    print(f"Event types: {events_df['event_type'].nunique()}")
    
    # Show team name standardization results
    print(f"\nTeam name standardization:")
    print(f"Hitting teams: {sorted(hitting_df['team'].unique())}")
    print(f"Pitching teams: {sorted(pitching_df['team'].unique())}")
    
    # Show some examples
    print(f"\nEvent type distribution:")
    print(events_df['event_type'].value_counts())

if __name__ == "__main__":
    clean_mlb_data()