import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import re
import logging
from typing import Dict, List, Optional, Tuple
import os

class BaseballScraper:
    # Web scraper for Baseball Almanac historical data
    def __init__(self, delay: float = 1.5):
 
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
        
    def setup_logging(self):
        # Setup logging configuration
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
        """
        Generate URL for a specific year
        
        Args:
            year: Year to scrape (e.g., 1927)
            
        Returns:
            Full URL for the year page
        """
        return f"{self.base_url}yr{year}a.shtml"
    
    def fetch_year_page(self, year: int) -> Optional[BeautifulSoup]:
        """
        Fetch and parse a year page
        
        Args:
            year: Year to fetch
            
        Returns:
            BeautifulSoup object or None if failed
        """
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
    
    def parse_hitting_leaders(self, soup: BeautifulSoup, year: int) -> List[Dict]:
        """
        Parse hitting leaders from the page
        
        Args:
            soup: BeautifulSoup object of the page
            year: Year being processed
            
        Returns:
            List of hitting leader records
        """
        hitting_records = []
        
        try:
            # Look for tables containing hitting statistics
            tables = soup.find_all('table')
            
            for table in tables:
                table_text = table.get_text().lower()
                
                # Check if this table contains hitting data
                if any(keyword in table_text for keyword in ['batting', 'hits', 'home run', 'rbi']):
                    rows = table.find_all('tr')
                    
                    for i, row in enumerate(rows):
                        cells = row.find_all(['td', 'th'])
                        
                        if len(cells) >= 3:  # Need at least rank, name, stat
                            try:
                                # Extract data from cells
                                cell_texts = [cell.get_text(strip=True) for cell in cells]
                                
                                # Look for player names (capital letters pattern)
                                for j, cell_text in enumerate(cell_texts):
                                    if re.match(r'^[A-Z][a-z]+ [A-Z][a-z]+', cell_text):
                                        player_name = cell_text
                                        
                                        # Try to extract rank (usually first column)
                                        rank = None
                                        if j > 0 and cell_texts[j-1].isdigit():
                                            rank = int(cell_texts[j-1])
                                        
                                        # Try to extract team (often after player name)
                                        team = ""
                                        if j + 1 < len(cell_texts):
                                            team = cell_texts[j + 1]
                                        
                                        # Try to extract statistic (numerical value)
                                        stat_value = None
                                        stat_category = "Unknown"
                                        
                                        for k, cell in enumerate(cell_texts):
                                            # Look for numerical statistics
                                            if re.match(r'^\d+$', cell) and k > j:  # Integer stats
                                                stat_value = int(cell)
                                                break
                                            elif re.match(r'^\.\d{3}$', cell):  # Batting average
                                                stat_value = float(cell)
                                                stat_category = "Batting Average"
                                                break
                                        
                                        if stat_value is not None:
                                            hitting_records.append({
                                                'year': year,
                                                'rank': rank,
                                                'player_name': player_name,
                                                'team': team,
                                                'stat_category': stat_category,
                                                'stat_value': stat_value
                                            })
                                            
                            except (ValueError, IndexError) as e:
                                # Skip malformed rows
                                continue
                                
        except Exception as e:
            self.logger.error(f"Error parsing hitting leaders for {year}: {e}")
        
        self.logger.info(f"Found {len(hitting_records)} hitting records for {year}")
        return hitting_records
    
    def parse_pitching_leaders(self, soup: BeautifulSoup, year: int) -> List[Dict]:
        """
        Parse pitching leaders from the page
        
        Args:
            soup: BeautifulSoup object of the page
            year: Year being processed
            
        Returns:
            List of pitching leader records
        """
        pitching_records = []
        
        try:
            tables = soup.find_all('table')
            
            for table in tables:
                table_text = table.get_text().lower()
                
                # Check if this table contains pitching data
                if any(keyword in table_text for keyword in ['pitching', 'era', 'wins', 'strikeout']):
                    rows = table.find_all('tr')
                    
                    for row in rows:
                        cells = row.find_all(['td', 'th'])
                        
                        if len(cells) >= 3:
                            try:
                                cell_texts = [cell.get_text(strip=True) for cell in cells]
                                
                                # Look for player names
                                for j, cell_text in enumerate(cell_texts):
                                    if re.match(r'^[A-Z][a-z]+ [A-Z][a-z]+', cell_text):
                                        player_name = cell_text
                                        
                                        # Extract rank
                                        rank = None
                                        if j > 0 and cell_texts[j-1].isdigit():
                                            rank = int(cell_texts[j-1])
                                        
                                        # Extract team
                                        team = ""
                                        if j + 1 < len(cell_texts):
                                            team = cell_texts[j + 1]
                                        
                                        # Extract statistics
                                        stat_value = None
                                        stat_category = "Unknown"
                                        
                                        for cell in cell_texts[j+1:]:
                                            # ERA pattern (x.xx)
                                            if re.match(r'^\d\.\d{2}$', cell):
                                                stat_value = float(cell)
                                                stat_category = "ERA"
                                                break
                                            # Wins, strikeouts (integers)
                                            elif re.match(r'^\d+$', cell):
                                                stat_value = int(cell)
                                                break
                                        
                                        if stat_value is not None:
                                            pitching_records.append({
                                                'year': year,
                                                'rank': rank,
                                                'player_name': player_name,
                                                'team': team,
                                                'stat_category': stat_category,
                                                'stat_value': stat_value
                                            })
                                            
                            except (ValueError, IndexError):
                                continue
                                
        except Exception as e:
            self.logger.error(f"Error parsing pitching leaders for {year}: {e}")
        
        self.logger.info(f"Found {len(pitching_records)} pitching records for {year}")
        return pitching_records
    
    def scrape_year(self, year: int) -> bool:
        """
        Scrape all data for a specific year
        
        Args:
            year: Year to scrape
            
        Returns:
            True if successful, False otherwise
        """
        self.logger.info(f"Starting scrape for year {year}")
        
        # Fetch the page
        soup = self.fetch_year_page(year)
        if not soup:
            return False
        
        # Parse different types of data
        hitting_data = self.parse_hitting_leaders(soup, year)
        pitching_data = self.parse_pitching_leaders(soup, year)
        
        # Add to our collections
        self.hitting_data.extend(hitting_data)
        self.pitching_data.extend(pitching_data)
        
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
    
    def scrape_test_years(self, years: List[int] = [1927, 2000, 2023]):
        """
        Scrape a few test years to validate the scraper
        
        Args:
            years: List of years to test with
        """
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
        
        return len(successful_years) > 0

if __name__ == "__main__":
    print("Baseball Almanac Scraper - Test Run")
    print("="*50)
    
    # Create scraper instance
    scraper = BaseballScraper(delay=1.5)
    
    # Run test scrape
    success = scraper.scrape_test_years([1927, 2000, 2023])
    
    if success:
        print("\nTest scraping completed successfully!")
        print("Check data/raw/ directory for CSV files")
        print("Check scraper.log for detailed logs")
    else:
        print("\nTest scraping failed - check logs for details")