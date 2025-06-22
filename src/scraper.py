import requests
from bs4 import BeautifulSoup, NavigableString
import pandas as pd
import time
import re
import logging
from typing import Dict, List, Optional, Tuple
import os

class ImprovedBaseballScraper:
    """
    Improved web scraper for Baseball Almanac historical data
    Better parsing of statistics categories and team standings
    """    
    def __init__(self, delay: float = 1.5):
        """Initialize the improved scraper"""
        self.base_url = "https://www.baseball-almanac.com/yearly/"
        self.delay = delay
        self.session = requests.Session()

        # Set headers to appear as a regular browser
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
        
        # Setup logging
        self.setup_logging()
        
        # Data containers
        self.hitting_data = []
        self.pitching_data = []
        self.standings_data = []
        self.events_data = []

        # Statistical category mappings
        self.hitting_categories = {
            'batting average': 'Batting Average',
            'home runs': 'Home Runs',
            'runs batted in': 'RBI',
            'rbi': 'RBI',
            'hits': 'Hits',
            'runs': 'Runs',
            'doubles': 'Doubles',
            'triples': 'Triples',
            'stolen bases': 'Stolen Bases',
            'total bases': 'Total Bases'
        }
        
        self.pitching_categories = {
            'earned run average': 'ERA',
            'era': 'ERA',
            'wins': 'Wins',
            'strikeouts': 'Strikeouts',
            'complete games': 'Complete Games',
            'shutouts': 'Shutouts',
            'saves': 'Saves',
            'innings pitched': 'Innings Pitched'
        }
        
    def setup_logging(self):
        """Setup logging configuration"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('scraper.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
        
    def get_year_url(self, year: int) -> str:
        """Generate URL for a specific year"""
        return f"{self.base_url}yr{year}a.shtml"
    
    def fetch_year_page(self, year: int) -> Optional[BeautifulSoup]:
        """Fetch and parse a year page"""
        url = self.get_year_url(year)
        
        try:
            self.logger.info(f"Fetching data for year {year}")
            
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            
            # Wait to be respectful to the server 
            time.sleep(self.delay)
            
            soup = BeautifulSoup(response.content, 'html.parser')
            self.logger.info(f"Successfully fetched {year} - Page title: {soup.title.string if soup.title else 'No title'}")
            
            return soup
            
        except requests.RequestException as e:
            self.logger.error(f"Failed to fetch {year}: {e}")
            return None
        except Exception as e:
            self.logger.error(f"Unexpected error fetching {year}: {e}")
            return None

    def identify_stat_category_improved(self, table_text: str, context_text: str) -> str:
        """
        IMPROVED: Better statistical category identification
        """
        combined_text = (table_text + " " + context_text).lower()
        
        # More specific hitting category detection
        if 'home run' in combined_text:
            return "Home Runs"
        elif 'runs batted in' in combined_text or 'rbi' in combined_text:
            return "RBI"
        elif 'batting average' in combined_text:
            return "Batting Average"
        elif 'total bases' in combined_text:
            return "Total Bases"
        elif 'stolen base' in combined_text:
            return "Stolen Bases"
        elif 'double' in combined_text and 'play' not in combined_text:
            return "Doubles"
        elif 'triple' in combined_text:
            return "Triples"
        elif 'hit' in combined_text and 'pitcher' not in combined_text:
            return "Hits"
        elif 'run' in combined_text and 'home' not in combined_text:
            return "Runs"
        
        # More specific pitching category detection
        elif 'earned run average' in combined_text or 'era' in combined_text:
            return "ERA"
        elif 'complete game' in combined_text:
            return "Complete Games"
        elif 'strikeout' in combined_text:
            return "Strikeouts"
        elif 'shutout' in combined_text:
            return "Shutouts"
        elif 'save' in combined_text:
            return "Saves"
        elif 'win' in combined_text and ('pitcher' in combined_text or 'pitching' in combined_text):
            return "Wins"
        elif 'inning' in combined_text:
            return "Innings Pitched"
        
        return "Unknown"
        
    def parse_table_with_context(self, table, surrounding_text: str = "") -> Tuple[List[Dict], str]:
        """
        FIXED: Parse a table with better context awareness and correct field mapping
        """
        records = []
        table_text = table.get_text().lower()
        
        # Determine if this is hitting or pitching data
        is_hitting = any(keyword in table_text for keyword in 
                        ['batting', 'hits', 'home run', 'rbi', 'doubles', 'triples'])
        is_pitching = any(keyword in table_text for keyword in 
                         ['pitching', 'era', 'wins', 'strikeout', 'complete'])
        
        if not (is_hitting or is_pitching):
            return records, "unknown"
        
        table_type = "hitting" if is_hitting else "pitching"
        
        # Get statistical category from context - IMPROVED
        stat_category = self.identify_stat_category_improved(table_text, surrounding_text)
        
        rows = table.find_all('tr')
        
        for row in rows:
            cells = row.find_all(['td', 'th'])
            
            if len(cells) >= 3:  # Need at least rank, name, value
                try:
                    cell_texts = [cell.get_text(strip=True) for cell in cells]
                    
                    # FIXED: Better player name detection
                    player_name = None
                    player_idx = -1
                    
                    for i, cell_text in enumerate(cell_texts):
                        cell_clean = cell_text.strip()
                        
                        # More robust player name pattern
                        if (re.match(r'^[A-Z][a-z]+ [A-Z][a-z]+(\s[A-Z][a-z]*)?$', cell_clean) and 
                            len(cell_clean) > 5 and 
                            not any(word in cell_clean.lower() for word in ['batting', 'era', 'average', 'runs'])):
                            player_name = cell_clean
                            player_idx = i
                            break
                    
                    if not player_name:
                        continue
                    
                    # FIXED: Extract rank (look in multiple positions)
                    rank = None
                    for i in range(max(0, player_idx - 2), player_idx):
                        if i < len(cell_texts) and cell_texts[i].strip().isdigit():
                            rank = int(cell_texts[i].strip())
                            break
                    
                    # FIXED: Extract team (more robust team detection)
                    team = ""
                    # Look after player name for team
                    for i in range(player_idx + 1, min(len(cell_texts), player_idx + 3)):
                        potential_team = cell_texts[i].strip()
                        
                        # Team criteria: short text, not a number, not a stat
                        if (len(potential_team) <= 15 and 
                            not potential_team.replace('.', '').replace(',', '').isdigit() and
                            not re.match(r'^\.\d{3}$', potential_team) and
                            not re.match(r'^\d\.\d{2}$', potential_team)):
                            team = potential_team
                            break
                    
                    # FIXED: Extract statistical value (look in right places)
                    stat_value = None
                    stat_category_detected = stat_category
                    
                    # Look for numerical values after the player/team info
                    for i in range(player_idx + 1, len(cell_texts)):
                        cell = cell_texts[i].strip()
                        
                        # Skip team names
                        if cell == team:
                            continue
                        
                        # Batting average (e.g., .356, .300)
                        if re.match(r'^\.\d{3}$', cell):
                            stat_value = float(cell)
                            stat_category_detected = "Batting Average"
                            break
                        
                        # ERA (e.g., 2.63, 3.45)
                        elif re.match(r'^\d\.\d{2}$', cell):
                            stat_value = float(cell)
                            stat_category_detected = "ERA"
                            break
                        
                        # Integer stats (home runs, RBI, wins, etc.)
                        elif re.match(r'^\d+$', cell):
                            value = int(cell)
                            
                            # Use context to determine what this number represents
                            if stat_category_detected == "Unknown":
                                # Guess based on value ranges and table context
                                if 'home run' in table_text and 20 <= value <= 80:
                                    stat_category_detected = "Home Runs"
                                elif 'rbi' in table_text and 50 <= value <= 200:
                                    stat_category_detected = "RBI"
                                elif 'hit' in table_text and value > 100:
                                    stat_category_detected = "Hits"
                                elif 'run' in table_text and not 'home' in table_text and 50 <= value <= 150:
                                    stat_category_detected = "Runs"
                                elif 'win' in table_text and is_pitching and 10 <= value <= 35:
                                    stat_category_detected = "Wins"
                                elif 'strikeout' in table_text and value > 50:
                                    stat_category_detected = "Strikeouts"
                                elif 'complete' in table_text and value <= 40:
                                    stat_category_detected = "Complete Games"
                                else:
                                    # Generic category based on table type
                                    stat_category_detected = "Hitting Stat" if is_hitting else "Pitching Stat"
                            
                            stat_value = value
                            break
                    
                    # Only add if we have a valid stat value
                    if stat_value is not None and stat_value != 0:
                        records.append({
                            'rank': rank,
                            'player_name': player_name,
                            'team': team,
                            'stat_category': stat_category_detected,
                            'stat_value': stat_value
                        })
                        
                except (ValueError, IndexError) as e:
                    # Skip malformed rows
                    continue
                    
        return records, table_type

    def parse_hitting_leaders(self, soup: BeautifulSoup, year: int) -> List[Dict]:
        """Parse hitting leaders from the page"""
        hitting_records = []
        
        try:
            # Find all tables
            tables = soup.find_all('table')
            
            for i, table in enumerate(tables):
                # Get surrounding context (text before/after table)
                surrounding_elements = []
                if table.previous_sibling:
                    surrounding_elements.append(str(table.previous_sibling))
                if table.next_sibling:
                    surrounding_elements.append(str(table.next_sibling))
                
                surrounding_text = ' '.join(surrounding_elements)
                
                records, table_type = self.parse_table_with_context(table, surrounding_text)
                
                if table_type == "hitting":
                    # Add year and extend records
                    for record in records:
                        record['year'] = year
                    hitting_records.extend(records)
                    
        except Exception as e:
            self.logger.error(f"Error parsing hitting leaders for {year}: {e}")
        
        self.logger.info(f"Found {len(hitting_records)} hitting records for {year}")
        return hitting_records
    
    def parse_pitching_leaders(self, soup: BeautifulSoup, year: int) -> List[Dict]:
        """Parse pitching leaders with improved accuracy"""
        pitching_records = []
        
        try:
            tables = soup.find_all('table')
            
            for table in tables:
                # Get surrounding context
                surrounding_elements = []
                if table.previous_sibling:
                    surrounding_elements.append(str(table.previous_sibling))
                if table.next_sibling:
                    surrounding_elements.append(str(table.next_sibling))
                
                surrounding_text = ' '.join(surrounding_elements)
                
                records, table_type = self.parse_table_with_context(table, surrounding_text)
                
                if table_type == "pitching":
                    # Add year and extend records
                    for record in records:
                        record['year'] = year
                    pitching_records.extend(records)
                    
        except Exception as e:
            self.logger.error(f"Error parsing pitching leaders for {year}: {e}")
        
        self.logger.info(f"Found {len(pitching_records)} pitching records for {year}")
        return pitching_records
    
    def parse_team_standings(self, soup: BeautifulSoup, year: int) -> List[Dict]:
        """Parse team standings and final records"""
        standings_records = []
        
        try:
            # Look for standings tables - they often contain team names and W-L records
            tables = soup.find_all('table')
            
            for table in tables:
                table_text = table.get_text().lower()
                
                # Check if this looks like a standings table
                if any(keyword in table_text for keyword in ['standings', 'final', 'won', 'lost', 'pct']):
                    rows = table.find_all('tr')
                    
                    for row in rows:
                        cells = row.find_all(['td', 'th'])
                        
                        if len(cells) >= 4:  # Team, Wins, Losses, Pct
                            try:
                                cell_texts = [cell.get_text(strip=True) for cell in cells]
                                
                                # Look for team names
                                team_name = None
                                for cell in cell_texts:
                                    # Team names are usually longer and contain common team words
                                    if len(cell) > 4 and any(word in cell.lower() for word in 
                                        ['yankees', 'red sox', 'athletics', 'tigers', 'cardinals', 'giants']):
                                        team_name = cell
                                        break
                                
                                if team_name:
                                    # Extract wins, losses, percentage
                                    wins = losses = win_pct = None
                                    
                                    for cell in cell_texts:
                                        # Look for win-loss record patterns
                                        if re.match(r'^\d{2,3}$', cell):  # Wins or losses (2-3 digits)
                                            if wins is None:
                                                wins = int(cell)
                                            elif losses is None:
                                                losses = int(cell)
                                        elif re.match(r'^\.\d{3}$', cell):  # Winning percentage
                                            win_pct = float(cell)
                                    
                                    if wins is not None and losses is not None:
                                        standings_records.append({
                                            'year': year,
                                            'team_name': team_name,
                                            'wins': wins,
                                            'losses': losses,
                                            'win_pct': win_pct or (wins / (wins + losses)) if (wins + losses) > 0 else None
                                        })
                                        
                            except (ValueError, IndexError):
                                continue
                                
        except Exception as e:
            self.logger.error(f"Error parsing team standings for {year}: {e}")
        
        self.logger.info(f"Found {len(standings_records)} team standings for {year}")
        return standings_records
    
    def scrape_year(self, year: int) -> bool:
        """Scrape all data for a specific year"""
        self.logger.info(f"Starting improved scrape for year {year}")
        
        # Fetch the page
        soup = self.fetch_year_page(year)
        if not soup:
            return False
        
        # Parse different types of data
        hitting_data = self.parse_hitting_leaders(soup, year)
        pitching_data = self.parse_pitching_leaders(soup, year)
        standings_data = self.parse_team_standings(soup, year)
        
        # Add to our collections
        self.hitting_data.extend(hitting_data)
        self.pitching_data.extend(pitching_data)
        self.standings_data.extend(standings_data)
        
        self.logger.info(f"Completed scrape for year {year}")
        return True
    
    def save_to_csv(self):
        """Save all collected data to CSV files"""
        
        # Create data directory if it doesn't exist
        os.makedirs('data/raw', exist_ok=True)
        
        # Save hitting data
        if self.hitting_data:
            hitting_df = pd.DataFrame(self.hitting_data)
            hitting_df.to_csv('data/raw/yearly_hitting_leaders.csv', index=False)
            self.logger.info(f"Saved {len(self.hitting_data)} hitting records to CSV")
        
        # Save pitching data
        if self.pitching_data:
            pitching_df = pd.DataFrame(self.pitching_data)
            pitching_df.to_csv('data/raw/yearly_pitching_leaders.csv', index=False)
            self.logger.info(f"Saved {len(self.pitching_data)} pitching records to CSV")

        # Save standings data
        if self.standings_data:
            standings_df = pd.DataFrame(self.standings_data)
            standings_df.to_csv('data/raw/team_standings.csv', index=False)
            self.logger.info(f"Saved {len(self.standings_data)} team standings to CSV")
    
    def scrape_test_years(self, years: List[int] = [1927, 2000, 2023]):
        """Test the improved scraper"""
        self.logger.info(f"Starting test scrape for years: {years}")
        
        successful_years = []
        failed_years = []
        
        for year in years:
            try:
                if self.scrape_year(year):
                    successful_years.append(year)
                else:
                    failed_years.append(year)
            except Exception as e:
                self.logger.error(f"Unexpected error scraping {year}: {e}")
                failed_years.append(year)
        
        # Save results
        self.save_to_csv()
        
        # Report results
        print(f"\nSCRAPING TEST RESULTS:")
        print(f"Successful years: {successful_years}")
        print(f"Failed years: {failed_years}")
        print(f"Total hitting records collected: {len(self.hitting_data)}")
        print(f"Total pitching records collected: {len(self.pitching_data)}")
        print(f"Total team standings: {len(self.standings_data)}")

        # Show sample data
        if self.hitting_data:
            print(f"\nSample hitting data:")
            for record in self.hitting_data[:3]:
                print(f"  {record['year']}: {record['player_name']} ({record['team']}) - {record['stat_category']}: {record['stat_value']}")
        
        if self.pitching_data:
            print(f"\nSample pitching data:")
            for record in self.pitching_data[:3]:
                print(f"  {record['year']}: {record['player_name']} ({record['team']}) - {record['stat_category']}: {record['stat_value']}")
        
        return len(successful_years) > 0

if __name__ == "__main__":
    print("Improved Baseball Almanac Scraper - Test Run")
    print("="*50)
    
    # Create scraper instance
    scraper = ImprovedBaseballScraper(delay=1.5)
    
    # Run test scrape
    success = scraper.scrape_test_years([1927, 2000, 2023])
    
    if success:
        print("\nTest scraping completed successfully!")
        print("Check data/raw/ directory for CSV files")
        print("Check scraper.log for detailed logs")
    else:
        print("\nImproved test scraping failed - check logs for details")