import os
import time
import pandas as pd
import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException
import random
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class EnhancedMLBScraper:
    def __init__(self):
        self.hitting_data = []
        self.pitching_data = []
        self.events_data = []
        self.standings_data = []
        self.driver = None
        self.session = requests.Session()
        self.setup_session()
        
        # Scraping statistics
        self.stats = {
            'pages_scraped': 0,
            'successful_requests': 0,
            'failed_requests': 0,
            'selenium_fallbacks': 0,
            'data_points_collected': 0
        }
    
    def setup_session(self):
        """Setup requests session with headers and retry strategy"""
        user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        ]
        
        self.session.headers.update({
            'User-Agent': random.choice(user_agents),
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        })
    
    def setup_driver(self):
        """Setup Selenium WebDriver with optimal configuration"""
        if self.driver:
            return
            
        try:
            options = Options()
            options.add_argument("--headless")
            options.add_argument("--no-sandbox")
            options.add_argument("--disable-dev-shm-usage")
            options.add_argument("--disable-gpu")
            options.add_argument("--window-size=1920,1080")
            options.add_argument("--disable-blink-features=AutomationControlled")
            options.add_experimental_option("excludeSwitches", ["enable-automation"])
            options.add_experimental_option('useAutomationExtension', False)
            
            # Rotate user agents
            user_agents = [
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebDriver/537.36',
                'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebDriver/537.36',
                'Mozilla/5.0 (X11; Linux x86_64) AppleWebDriver/537.36'
            ]
            options.add_argument(f"--user-agent={random.choice(user_agents)}")
            
            self.driver = webdriver.Chrome(options=options)
            self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            logger.info("Selenium WebDriver initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize WebDriver: {e}")
            self.driver = None
    
    def scrape_with_requests(self, url: str, timeout: int = 10) -> BeautifulSoup:
        """Try scraping with requests first (faster)"""
        try:
            # Rotate user agent
            user_agents = [
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
            ]
            self.session.headers['User-Agent'] = random.choice(user_agents)
            
            response = self.session.get(url, timeout=timeout)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            self.stats['successful_requests'] += 1
            return soup
            
        except Exception as e:
            logger.warning(f"Requests failed for {url}: {e}")
            self.stats['failed_requests'] += 1
            return None
    
    def scrape_with_selenium(self, url: str, timeout: int = 15) -> BeautifulSoup:
        """Fallback to Selenium for dynamic content"""
        try:
            if not self.driver:
                self.setup_driver()
            
            if not self.driver:
                logger.error("WebDriver not available")
                return None
            
            self.driver.get(url)
            
            # Wait for page to load
            WebDriverWait(self.driver, timeout).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
            
            # Additional wait for dynamic content
            time.sleep(2)
            
            soup = BeautifulSoup(self.driver.page_source, 'html.parser')
            self.stats['selenium_fallbacks'] += 1
            return soup
            
        except (TimeoutException, WebDriverException) as e:
            logger.error(f"Selenium failed for {url}: {e}")
            return None
    
    def scrape_page(self, url: str) -> BeautifulSoup:
        """Main scraping method with fallback strategy"""
        logger.info(f"Scraping: {url}")
        
        # Try requests first
        soup = self.scrape_with_requests(url)
        
        # Fallback to Selenium if requests fails
        if soup is None:
            logger.info("Falling back to Selenium...")
            soup = self.scrape_with_selenium(url)
        
        if soup:
            self.stats['pages_scraped'] += 1
        
        # Add delay between requests
        time.sleep(random.uniform(1, 3))
        
        return soup
    
    def scrape_year(self, year: int):
        """Scrape all data for a specific year with enhanced error handling"""
        url = f"https://www.baseball-almanac.com/yearly/yr{year}a.shtml"
        logger.info(f"Starting scrape for year {year}")
        
        soup = self.scrape_page(url)
        
        if not soup:
            logger.error(f"Failed to scrape {year} - no content retrieved")
            return False
        
        try:
            # Extract different types of data
            events_found = self.extract_events(soup, year)
            tables_found = self.parse_all_tables(soup, year)
            
            logger.info(f"Year {year} completed: {events_found} events, {tables_found} tables processed")
            return True
            
        except Exception as e:
            logger.error(f"Error processing {year}: {e}")
            return False
    
    def extract_events(self, soup: BeautifulSoup, year: int) -> int:
        """Enhanced event extraction with better filtering"""
        events_found = 0
        processed_texts = set()
        
        # Look for main content areas
        content_selectors = [
            'div.content', 'div.main', 'div.article',
            'div#content', 'div#main', 'div.post'
        ]
        
        main_content = None
        for selector in content_selectors:
            main_content = soup.select_one(selector)
            if main_content:
                break
        
        if not main_content:
            main_content = soup
        
        # Extract from paragraphs
        paragraphs = main_content.find_all('p')
        
        for p in paragraphs:
            text = p.get_text(strip=True)
            
            if not self.is_valid_event_text(text, processed_texts):
                continue
            
            # Clean and classify the event
            cleaned_text = self.clean_event_text(text)
            event_type = self.classify_event_enhanced(cleaned_text)
            
            if cleaned_text and len(cleaned_text) > 50:  # Minimum length for meaningful events
                self.events_data.append({
                    'year': year,
                    'description': cleaned_text,
                    'event_type': event_type
                })
                processed_texts.add(cleaned_text)
                events_found += 1
                self.stats['data_points_collected'] += 1
        
        return events_found
    
    def is_valid_event_text(self, text: str, processed_texts: set) -> bool:
        """Enhanced validation for event text"""
        if not text or len(text) < 30:
            return False
        
        if text in processed_texts:
            return False
        
        # Skip navigation and non-content
        skip_patterns = [
            r'baseball almanac',
            r'copyright',
            r'all rights reserved',
            r'find us on',
            r'follow @',
            r'stats awards',
            r'hosting 4 less',
            r'where what happened',
            r'player review',
            r'pitcher review',
            r'team standings',
            r'top 25',
            r'ballplayers autographs',
            r'left field1,500'
        ]
        
        text_lower = text.lower()
        for pattern in skip_patterns:
            if pattern in text_lower:
                return False
        
        # Must contain baseball-related content
        baseball_indicators = [
            'game', 'season', 'player', 'pitcher', 'hitter', 'baseball',
            'home run', 'strikeout', 'hit', 'world series', 'record',
            'debut', 'retire', 'no-hitter', 'yankees', 'red sox',
            'league', 'major league', 'american league', 'national league'
        ]
        
        if not any(indicator in text_lower for indicator in baseball_indicators):
            return False
        
        # Filter out non-baseball historical events
        non_baseball_terms = [
            'earthquake', 'president', 'politics', 'war', 'murder',
            'execution', 'european union', 'space shuttle', 'olympic'
        ]
        
        if any(term in text_lower for term in non_baseball_terms):
            return False
        
        return True
    
    def classify_event_enhanced(self, text: str) -> str:
        """Enhanced event classification with more specific categories"""
        text_lower = text.lower()
        
        # Priority-based classification (most specific first)
        classifications = [
            ('World Series', ['world series', 'championship series', 'swept', 'game 7']),
            ('No-Hitter', ['no-hitter', 'no-hit', 'perfect game', 'no hitter']),
            ('Record', ['record', 'first player to', 'first time', 'most', 'fastest', 'longest', 'broke the record', 'set a new', 'all-time']),
            ('Debut', ['debut', 'first game', 'first appearance', 'rookie', 'first african-american', 'first black player', 'expansion']),
            ('Retirement', ['retire', 'retirement', 'final game', 'last season', 'announced retirement', 'career ended']),
            ('Death', ['death', 'died', 'passed away']),
            ('Award', ['mvp', 'most valuable player', 'cy young', 'rookie of the year', 'hall of fame', 'award']),
            ('Transaction', ['trade', 'traded', 'acquired', 'signed', 'contract']),
            ('Rule Change', ['rule', 'designated hitter', 'mound', 'strike zone', 'expansion', 'playoff format']),
            ('Milestone', ['3000', '500', '400', 'milestone', 'career', 'thousandth'])
        ]
        
        for event_type, keywords in classifications:
            if any(keyword in text_lower for keyword in keywords):
                return event_type
        
        return 'Notable Event'
    
    def parse_all_tables(self, soup: BeautifulSoup, year: int) -> int:
        """Parse all relevant tables with better identification"""
        tables_processed = 0
        tables = soup.find_all('table')
        
        for table in tables:
            table_type = self.identify_table_type(table, year)
            
            if table_type == 'hitting':
                if self.parse_statistical_table(table, year, 'hitting'):
                    tables_processed += 1
            elif table_type == 'pitching':
                if self.parse_statistical_table(table, year, 'pitching'):
                    tables_processed += 1
            elif table_type == 'standings':
                if self.parse_standings_table(table, year):
                    tables_processed += 1
        
        return tables_processed
    
    def identify_table_type(self, table, year: int) -> str:
        """Better table type identification"""
        # Get context around the table
        context = ""
        
        # Check previous siblings for headers
        prev_element = table.find_previous_sibling()
        if prev_element:
            context += prev_element.get_text().lower()
        
        # Check parent for headers
        parent = table.parent
        if parent:
            headers = parent.find_all(['h1', 'h2', 'h3', 'h4'])
            for header in headers:
                context += " " + header.get_text().lower()
        
        # Check table headers
        first_row = table.find('tr')
        if first_row:
            context += " " + first_row.get_text().lower()
        
        # Classify based on context
        if any(keyword in context for keyword in ['player review', 'hitting', 'batting']) and 'pitcher' not in context:
            return 'hitting'
        elif any(keyword in context for keyword in ['pitcher review', 'pitching']):
            return 'pitching'
        elif any(keyword in context for keyword in ['standings', 'team', 'wins', 'losses']):
            return 'standings'
        
        return 'unknown'
    
    def parse_statistical_table(self, table, year: int, table_type: str) -> bool:
        """Parse hitting or pitching statistics tables"""
        try:
            rows = table.find_all('tr')
            if len(rows) < 2:
                return False
            
            # Parse data rows (skip header)
            records_added = 0
            for row in rows[1:]:
                cells = row.find_all(['td', 'th'])
                if len(cells) < 4:
                    continue
                
                values = [cell.get_text(strip=True) for cell in cells]
                
                # Expected format: [Statistic, Player, Team, Value, ...]
                if len(values) >= 4:
                    stat_category = values[0]
                    player_name = values[1]
                    team = values[2]
                    
                    try:
                        stat_value = float(values[3])
                        
                        if self.is_valid_player_record(player_name, stat_category, stat_value):
                            data_dict = {
                                'year': year,
                                'player_name': player_name,
                                'team': team,
                                'stat_category': stat_category,
                                'stat_value': stat_value
                            }
                            
                            if table_type == 'hitting':
                                self.hitting_data.append(data_dict)
                            else:
                                self.pitching_data.append(data_dict)
                            
                            records_added += 1
                            self.stats['data_points_collected'] += 1
                    
                    except (ValueError, IndexError):
                        continue
            
            logger.info(f"  Added {records_added} {table_type} records for {year}")
            return records_added > 0
            
        except Exception as e:
            logger.error(f"Error parsing {table_type} table for {year}: {e}")
            return False
    
    def is_valid_player_record(self, name: str, category: str, value: float) -> bool:
        """Validate player records with reasonable ranges"""
        if not name or len(name) < 3:
            return False
        
        # Skip obvious non-names
        skip_terms = ['statistic', 'name', 'team', 'league', 'total', 'average', 'leader']
        if any(term in name.lower() for term in skip_terms):
            return False
        
        # Validate statistical ranges
        ranges = {
            'Home Runs': (0, 100),
            'Batting Average': (0.100, 0.500),
            'RBI': (0, 200),
            'ERA': (0.00, 10.00),
            'Wins': (0, 35),
            'Strikeouts': (0, 400),
            'Saves': (0, 70)
        }
        
        if category in ranges:
            min_val, max_val = ranges[category]
            if not (min_val <= value <= max_val):
                return False
        
        return True
    
    def parse_standings_table(self, table, year: int) -> bool:
        """Parse team standings with validation"""
        try:
            rows = table.find_all('tr')
            if len(rows) < 2:
                return False
            
            records_added = 0
            for row in rows[1:]:
                cells = row.find_all(['td', 'th'])
                if len(cells) < 4:
                    continue
                
                values = [cell.get_text(strip=True) for cell in cells]
                
                if len(values) >= 4:
                    team_name = values[0]
                    
                    try:
                        wins = int(values[1])
                        losses = int(values[2])
                        
                        # Validate reasonable season totals
                        total_games = wins + losses
                        expected_games = 154 if year <= 1960 else (60 if year == 2020 else 162)
                        
                        if abs(total_games - expected_games) <= 12:  # Allow some variance
                            win_pct = wins / total_games
                            
                            self.standings_data.append({
                                'year': year,
                                'team_name': team_name,
                                'wins': wins,
                                'losses': losses,
                                'win_pct': round(win_pct, 3)
                            })
                            
                            records_added += 1
                            self.stats['data_points_collected'] += 1
                    
                    except (ValueError, IndexError):
                        continue
            
            logger.info(f"  Added {records_added} standings records for {year}")
            return records_added > 0
            
        except Exception as e:
            logger.error(f"Error parsing standings for {year}: {e}")
            return False
    
    def clean_event_text(self, text: str) -> str:
        """Clean event text more thoroughly"""
        # Remove extra whitespace
        text = ' '.join(text.split())
        
        # Remove common artifacts
        text = text.replace('""', '"').replace('"', '"').replace('"', '"')
        
        # Limit length but preserve complete sentences
        if len(text) > 400:
            # Try to end at a sentence boundary
            sentences = text[:400].split('.')
            if len(sentences) > 1:
                text = '.'.join(sentences[:-1]) + '.'
            else:
                text = text[:400] + '...'
        
        return text.strip()
    
    def save_data(self):
        """Save data with enhanced error handling and validation"""
        os.makedirs('data/raw', exist_ok=True)
        
        # Save with backup
        datasets = [
            ('yearly_hitting_leaders.csv', self.hitting_data, 
             ['year', 'player_name', 'team', 'stat_category', 'stat_value']),
            ('yearly_pitching_leaders.csv', self.pitching_data,
             ['year', 'player_name', 'team', 'stat_category', 'stat_value']),
            ('team_standings.csv', self.standings_data,
             ['year', 'team_name', 'wins', 'losses', 'win_pct']),
            ('notable_events.csv', self.events_data,
             ['year', 'description', 'event_type'])
        ]
        
        for filename, data, columns in datasets:
            filepath = f'data/raw/{filename}'
            
            if data:
                df = pd.DataFrame(data)
                df.to_csv(filepath, index=False)
                logger.info(f"Saved {len(data)} records to {filename}")
            else:
                # Create empty file with headers
                pd.DataFrame(columns=columns).to_csv(filepath, index=False)
                logger.warning(f"Created empty {filename}")
    
    def print_final_stats(self):
        """Print comprehensive scraping statistics"""
        logger.info("\n" + "="*60)
        logger.info("SCRAPING COMPLETED - FINAL STATISTICS")
        logger.info("="*60)
        logger.info(f"Pages scraped: {self.stats['pages_scraped']}")
        logger.info(f"Successful requests: {self.stats['successful_requests']}")
        logger.info(f"Failed requests: {self.stats['failed_requests']}")
        logger.info(f"Selenium fallbacks: {self.stats['selenium_fallbacks']}")
        logger.info(f"Total data points: {self.stats['data_points_collected']}")
        logger.info("")
        logger.info(f"Hitting records: {len(self.hitting_data)}")
        logger.info(f"Pitching records: {len(self.pitching_data)}")
        logger.info(f"Standings records: {len(self.standings_data)}")
        logger.info(f"Event records: {len(self.events_data)}")
        logger.info("="*60)
    
    def run(self, years: list):
        """Main execution method with comprehensive error handling"""
        logger.info("Starting Enhanced MLB Historical Data Scraper")
        logger.info(f"Target years: {years}")
        
        successful_years = []
        failed_years = []
        
        try:
            for year in years:
                logger.info(f"\n--- Processing Year {year} ---")
                
                success = self.scrape_year(year)
                
                if success:
                    successful_years.append(year)
                    logger.info(f"✓ Year {year} completed successfully")
                else:
                    failed_years.append(year)
                    logger.error(f"✗ Year {year} failed")
                
                # Progress update
                logger.info(f"Progress: {len(successful_years + failed_years)}/{len(years)} years processed")
            
            # Save all collected data
            self.save_data()
            
            # Print final statistics
            self.print_final_stats()
            
            if failed_years:
                logger.warning(f"Failed years: {failed_years}")
            
            logger.info("Scraping process completed!")
            
        except KeyboardInterrupt:
            logger.info("Scraping interrupted by user")
            self.save_data()
        
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            self.save_data()
        
        finally:
            if self.driver:
                self.driver.quit()
                logger.info("WebDriver closed")

if __name__ == "__main__":
    # Significant years in MLB history with clear rationale
    target_years = [
        1927,  # Murderers' Row Yankees, Babe Ruth 60 HRs
        1947,  # Jackie Robinson breaks color barrier
        1961,  # Maris breaks Ruth's record, AL expansion
        1969,  # End of "Year of the Pitcher", divisional play begins
        1994,  # Strike-shortened season, beginning of offensive explosion
        1998,  # McGwire/Sosa home run chase
        2001,  # Bonds' 73 HRs, post-9/11 season
        2016,  # Cubs break 108-year drought, analytics era
        2020,  # COVID-shortened season
        2023   # Modern rule changes (pitch clock, shift restrictions)
    ]
    
    scraper = EnhancedMLBScraper()
    scraper.run(target_years)