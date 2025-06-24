import os
import time
import pandas as pd
import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

class MLBScraper:
    def __init__(self):
        self.hitting_data = []
        self.pitching_data = []
        self.events_data = []
        self.standings_data = []
        self.setup_driver()
    
    def setup_driver(self):
        options = Options()
        options.add_argument("--headless")
        options.add_argument("--no-sandbox")
        options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
        self.driver = webdriver.Chrome(options=options)
    
    def scrape_year(self, year):
        url = f"https://www.baseball-almanac.com/yearly/yr{year}a.shtml"
        print(f"Scraping {year}...")
        
        try:
            # Try requests first
            headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
            response = requests.get(url, headers=headers, timeout=10)
            
            if response.status_code == 200:
                soup = BeautifulSoup(response.content, 'html.parser')
            else:
                # Fallback to Selenium
                print(f"  Using Selenium for {year}")
                self.driver.get(url)
                time.sleep(3)
                soup = BeautifulSoup(self.driver.page_source, 'html.parser')
        
        except:
            # Fallback to Selenium
            print(f"  Using Selenium for {year}")
            self.driver.get(url)
            time.sleep(3)
            soup = BeautifulSoup(self.driver.page_source, 'html.parser')
        
        # Extract events from paragraphs and text
        self.extract_events(soup, year)
        
        # Parse tables by their specific headers
        self.parse_targeted_tables(soup, year)
        
        time.sleep(2)
    
    def extract_events(self, soup, year):
        """Extract notable events from main content only"""
        events_found = 0
        processed_texts = set()  # To avoid duplicates
        
        # Look for main content areas first
        main_content = soup.find('div', {'class': ['content', 'main', 'article']}) or soup
        
        # Find paragraphs in the main content area
        paragraphs = main_content.find_all('p')
        
        for p in paragraphs:
            text = p.get_text(strip=True)
            
            # Skip if too short or already processed
            if len(text) < 30 or text in processed_texts:
                continue
            
            # Skip navigation and site elements
            if self.is_navigation_text(text):
                continue
            
            # Skip copyright and footer text
            if any(skip_word in text.lower() for skip_word in [
                'copyright', '1999-', 'baseball almanac', 'hosting 4 less',
                'all rights reserved', 'find us on facebook', 'follow @',
                'stats awards fabulous', 'where what happened yesterday'
            ]):
                continue
            
            # Skip table headers and navigation elements
            if any(skip_phrase in text.lower() for skip_phrase in [
                'hitting statistics league leaderboard',
                'pitching statistics league leaderboard', 
                'american league player review',
                'american league pitcher review',
                'team standings',
                'all-star game',
                'top 25'
            ]):
                continue
            
            # Only keep baseball-related content
            if self.is_baseball_event(text):
                # Clean the text
                cleaned_text = self.clean_event_text(text)
                
                if cleaned_text and cleaned_text not in processed_texts:
                    self.events_data.append({
                        'year': year,
                        'description': cleaned_text,
                        'event_type': self.classify_event(cleaned_text)
                    })
                    processed_texts.add(cleaned_text)
                    events_found += 1
        
        print(f"    Found {events_found} valid events for {year}")
    
    def is_navigation_text(self, text):
        """Check if text is from navigation/menu elements"""
        # Navigation indicators
        nav_indicators = [
            'baseball almanachistory',
            'playersbaseball families',
            'leadersattendance data',
            'left field1,500',
            'statsawardsfabulous',
            'peopleautographsballplayers'
        ]
        
        # Check for navigation patterns
        text_lower = text.lower().replace(' ', '')
        for indicator in nav_indicators:
            if indicator in text_lower:
                return True
        
        # Check for menu-like structure (lots of capitals without spaces)
        if len([c for c in text if c.isupper()]) > len(text) * 0.3:
            return True
            
        return False
    
    def is_baseball_event(self, text):
        """Check if text describes a real baseball event"""
        text_lower = text.lower()
        
        # Must contain baseball-related keywords
        baseball_keywords = [
            'baseball', 'game', 'season', 'player', 'pitcher', 'hitter',
            'home run', 'strikeout', 'hit', 'world series', 'record',
            'debut', 'retire', 'no-hitter', 'perfect game', 'yankees',
            'red sox', 'tigers', 'athletics', 'orioles', 'league',
            'american league', 'national league', 'major league'
        ]
        
        if not any(keyword in text_lower for keyword in baseball_keywords):
            return False
        
        # Should NOT contain non-baseball content
        non_baseball = [
            'earthquake', 'president', 'politics', 'war', 'lindbergh',
            'aviator', 'murder', 'execution', 'anarchist', 'italy',
            'space shuttle', 'olympic', 'european union'
        ]
        
        if any(word in text_lower for word in non_baseball):
            return False
        
        # Should contain proper sentence structure
        if not any(char in text for char in '.!?'):
            return False
            
        return True
    
    def clean_event_text(self, text):
        """Clean and format event text"""
        # Remove extra whitespace
        text = ' '.join(text.split())
        
        # Limit length
        if len(text) > 400:
            text = text[:400] + '...'
        
        # Remove quotes artifacts
        text = text.replace('""', '"')
        
        return text.strip()
    
    def classify_event(self, text):
        """Classify event type based on keywords with better accuracy"""
        text_lower = text.lower()
        
        # World Series (highest priority)
        if any(phrase in text_lower for phrase in ['world series', 'series winner', 'championship series']):
            return 'World Series'
        
        # No-hitters and perfect games
        elif any(phrase in text_lower for phrase in ['no-hitter', 'no-hit', 'perfect game', 'no hitter']):
            return 'No-Hitter'
        
        # Records (various types)
        elif any(phrase in text_lower for phrase in [
            'set a record', 'broke the record', 'new record', 'record for',
            'all-time record', 'major league record', 'first player',
            'first time', 'most', 'fastest', 'longest'
        ]):
            return 'Record'
        
        # Player debuts and firsts
        elif any(phrase in text_lower for phrase in [
            'debut', 'first game', 'first appearance', 'rookie',
            'first african-american', 'first black player', 'expansion'
        ]):
            return 'Debut'
        
        # Retirements and career endings
        elif any(phrase in text_lower for phrase in [
            'retire', 'retirement', 'final game', 'last season',
            'announced his retirement', 'career ended'
        ]):
            return 'Retirement'
        
        # Deaths and obituaries
        elif any(phrase in text_lower for phrase in ['death', 'died', 'passed away']):
            return 'Death'
        
        # Awards and honors
        elif any(phrase in text_lower for phrase in [
            'mvp', 'most valuable player', 'cy young', 'rookie of the year',
            'hall of fame', 'award', 'honor'
        ]):
            return 'Award'
        
        # Trades and transactions
        elif any(phrase in text_lower for phrase in ['trade', 'traded', 'acquired', 'signed']):
            return 'Transaction'
        
        # Otherwise, general notable event
        else:
            return 'Notable Event'
    
    def parse_targeted_tables(self, soup, year):
        """Parse tables based on their headers and context"""
        
        # Look for table containers and headers
        tables = soup.find_all('table')
        
        for table in tables:
            # Try to find the table header/title by looking at surrounding elements
            table_context = self.get_table_context(table)
            
            if 'player review' in table_context.lower() and 'pitcher' not in table_context.lower():
                print(f"  Found hitting table for {year}")
                self.parse_hitting_table(table, year)
            
            elif 'pitcher review' in table_context.lower():
                print(f"  Found pitching table for {year}")
                self.parse_pitching_table(table, year)
            
            elif 'team standings' in table_context.lower():
                print(f"  Found standings table for {year}")
                self.parse_standings_table(table, year)
    
    def get_table_context(self, table):
        """Get context around table to understand what it contains"""
        context = ""
        
        # Look at previous siblings for headers
        prev_element = table.find_previous_sibling()
        if prev_element:
            context += prev_element.get_text()
        
        # Look at parent elements
        parent = table.parent
        if parent:
            # Look for header text in parent
            headers = parent.find_all(['h1', 'h2', 'h3', 'h4'])
            for header in headers:
                context += " " + header.get_text()
        
        # Look at the table itself for clues
        first_row = table.find('tr')
        if first_row:
            context += " " + first_row.get_text()
        
        return context
    
    def parse_hitting_table(self, table, year):
        """Parse individual player hitting statistics"""
        rows = table.find_all('tr')
        
        # Skip if too few rows
        if len(rows) < 2:
            return
        
        # Find header row to understand column structure
        headers = []
        header_row = rows[0]
        header_cells = header_row.find_all(['th', 'td'])
        headers = [cell.get_text(strip=True).lower() for cell in header_cells]
        
        print(f"    Hitting table headers: {headers}")
        
        # Parse data rows
        for row in rows[1:]:
            cells = row.find_all(['td', 'th'])
            if len(cells) < 4:  # Need at least: Statistic, Name, Team, Value
                continue
            
            values = [cell.get_text(strip=True) for cell in cells]
            
            # Expected format: [Statistic, Name(s), Team(s), Value, ...]
            if len(values) >= 4:
                stat_category = values[0]
                player_name = values[1]
                team = values[2] if values[2] else ""
                
                try:
                    stat_value = float(values[3])
                    
                    # Only add if it looks like a valid player name
                    if self.is_valid_player_name(player_name):
                        self.hitting_data.append({
                            'year': year,
                            'player_name': player_name,
                            'team': team,
                            'stat_category': stat_category,
                            'stat_value': stat_value
                        })
                        print(f"      Added: {player_name} - {stat_category}: {stat_value}")
                
                except (ValueError, IndexError):
                    continue
    
    def parse_pitching_table(self, table, year):
        """Parse individual pitcher statistics"""
        rows = table.find_all('tr')
        
        if len(rows) < 2:
            return
        
        # Find header row
        headers = []
        header_row = rows[0]
        header_cells = header_row.find_all(['th', 'td'])
        headers = [cell.get_text(strip=True).lower() for cell in headers]
        
        print(f"    Pitching table headers: {headers}")
        
        # Parse data rows
        for row in rows[1:]:
            cells = row.find_all(['td', 'th'])
            if len(cells) < 4:
                continue
            
            values = [cell.get_text(strip=True) for cell in cells]
            
            if len(values) >= 4:
                stat_category = values[0]
                player_name = values[1]
                team = values[2] if values[2] else ""
                
                try:
                    stat_value = float(values[3])
                    
                    if self.is_valid_player_name(player_name):
                        self.pitching_data.append({
                            'year': year,
                            'player_name': player_name,
                            'team': team,
                            'stat_category': stat_category,
                            'stat_value': stat_value
                        })
                        print(f"      Added: {player_name} - {stat_category}: {stat_value}")
                
                except (ValueError, IndexError):
                    continue
    
    def parse_standings_table(self, table, year):
        """Parse team standings"""
        rows = table.find_all('tr')
        
        if len(rows) < 2:
            return
        
        print(f"    Parsing standings table with {len(rows)} rows")
        
        # Parse data rows (skip header)
        for row in rows[1:]:
            cells = row.find_all(['td', 'th'])
            if len(cells) < 4:
                continue
            
            values = [cell.get_text(strip=True) for cell in cells]
            
            # Expected format: [Team, W, L, WP, GB]
            if len(values) >= 4:
                team_name = values[0]
                
                try:
                    wins = int(values[1])
                    losses = int(values[2])
                    
                    # Only add if reasonable season totals
                    if 40 <= wins <= 130 and 40 <= losses <= 130:
                        win_pct = wins / (wins + losses)
                        
                        self.standings_data.append({
                            'year': year,
                            'team_name': team_name,
                            'wins': wins,
                            'losses': losses,
                            'win_pct': round(win_pct, 3)
                        })
                        print(f"      Added: {team_name} {wins}-{losses}")
                
                except (ValueError, IndexError):
                    continue
    
    def is_valid_player_name(self, name):
        """Check if text looks like a valid player name"""
        if not name or len(name) < 3:
            return False
        
        # Skip if it's clearly not a name
        skip_terms = ['statistic', 'name', 'team', 'league', 'total', 'average']
        if any(term in name.lower() for term in skip_terms):
            return False
        
        # Should look like "First Last" or "First Middle Last"
        words = name.split()
        if len(words) < 2:
            return False
        
        # Each word should start with capital letter
        return all(word[0].isupper() for word in words if word)
    
    def save_data(self):
        os.makedirs('data/raw', exist_ok=True)
        
        # Save hitting data
        if self.hitting_data:
            hitting_df = pd.DataFrame(self.hitting_data)
            hitting_df.to_csv('data/raw/yearly_hitting_leaders.csv', index=False)
            print(f"Saved {len(self.hitting_data)} hitting records")
        else:
            pd.DataFrame(columns=['year', 'player_name', 'team', 'stat_category', 'stat_value']).to_csv(
                'data/raw/yearly_hitting_leaders.csv', index=False)
            print("Created empty hitting file")
        
        # Save pitching data
        if self.pitching_data:
            pitching_df = pd.DataFrame(self.pitching_data)
            pitching_df.to_csv('data/raw/yearly_pitching_leaders.csv', index=False)
            print(f"Saved {len(self.pitching_data)} pitching records")
        else:
            pd.DataFrame(columns=['year', 'player_name', 'team', 'stat_category', 'stat_value']).to_csv(
                'data/raw/yearly_pitching_leaders.csv', index=False)
            print("Created empty pitching file")
        
        # Save standings
        if self.standings_data:
            standings_df = pd.DataFrame(self.standings_data)
            standings_df.to_csv('data/raw/team_standings.csv', index=False)
            print(f"Saved {len(self.standings_data)} standings records")
        else:
            pd.DataFrame(columns=['year', 'team_name', 'wins', 'losses', 'win_pct']).to_csv(
                'data/raw/team_standings.csv', index=False)
            print("Created empty standings file")
        
        # Save events
        if self.events_data:
            events_df = pd.DataFrame(self.events_data)
            events_df.to_csv('data/raw/notable_events.csv', index=False)
            print(f"Saved {len(self.events_data)} event records")
        else:
            pd.DataFrame(columns=['year', 'description', 'event_type']).to_csv(
                'data/raw/notable_events.csv', index=False)
            print("Created empty events file")
    
    def run(self, years):
        print("Starting improved MLB scraper...")
        
        for year in years:
            self.scrape_year(year)
        
        self.save_data()
        self.driver.quit()
        print("Scraping completed!")

if __name__ == "__main__":
    # Significant years in MLB history
    years = [1927, 1947, 1961, 1969, 1994, 1998, 2001, 2016, 2020, 2023]
    
    scraper = MLBScraper()
    scraper.run(years)