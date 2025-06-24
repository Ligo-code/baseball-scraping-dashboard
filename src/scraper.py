import os
import re
import time
import logging
import pandas as pd
from bs4 import BeautifulSoup
from typing import List, Optional
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


SIGNIFICANT_YEARS = [1927, 1947, 1961, 1969, 1994, 1998, 2001, 2016, 2020, 2023]


class HistoricalMLBScraper:
    def __init__(self, delay: float = 3.0, timeout: int = 30):
        self.base_url = "https://www.baseball-almanac.com/yearly/"
        self.delay = delay
        self.timeout = timeout
        self.setup_logging()
        self.setup_driver()
        self.setup_session()

        self.hitting_data = []
        self.pitching_data = []
        self.standings_data = []
        self.events_data = []

    def setup_logging(self):
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s",
            handlers=[logging.FileHandler("scraper.log"), logging.StreamHandler()],
        )
        self.logger = logging.getLogger(__name__)

    def setup_driver(self):
        try:
            options = Options()
            options.add_argument("--headless")
            options.add_argument("--disable-gpu")
            options.add_argument("--no-sandbox")
            options.add_argument("--disable-dev-shm-usage")
            options.add_argument("--disable-extensions")
            options.add_argument("--disable-images")
            options.add_argument("--disable-javascript")  # For static content
            options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
            options.add_argument("--remote-debugging-port=9222")
            options.add_argument("--disable-blink-features=AutomationControlled")
            
            # Increase timeouts
            options.add_argument(f"--timeout={self.timeout}")
            
            self.driver = webdriver.Chrome(options=options)
            self.driver.set_page_load_timeout(self.timeout)
            self.driver.implicitly_wait(10)
            self.logger.info("Chrome driver initialized successfully")
        except Exception as e:
            self.logger.warning(f"Chrome driver failed to initialize: {e}")
            self.logger.info("Will use requests-only mode")
            self.driver = None

    def setup_session(self):
        """Setup requests session with retry logic"""
        self.session = requests.Session()
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })

    def __del__(self):
        if hasattr(self, "driver") and self.driver:
            try:
                self.driver.quit()
            except:
                pass

    def get_year_url(self, year: int) -> str:
        return f"{self.base_url}yr{year}a.shtml"

    def fetch_year_page_requests(self, year: int) -> Optional[BeautifulSoup]:
        """Alternative method with requests (faster for static content)"""
        url = self.get_year_url(year)
        try:
            self.logger.info(f"Fetching page for year {year} via requests")
            response = self.session.get(url, timeout=self.timeout)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, "html.parser")
            time.sleep(self.delay)
            return soup
        except Exception as e:
            self.logger.error(f"Error fetching year {year} via requests: {e}")
            return None

    def fetch_year_page(self, year: int) -> Optional[BeautifulSoup]:
        """Main method with Selenium (fallback)"""
        if not self.driver:
            self.logger.warning("Selenium driver not available, skipping")
            return None
            
        url = self.get_year_url(year)
        try:
            self.logger.info(f"Fetching page for year {year} via Selenium")
            self.driver.get(url)
            
            # Wait for main content to load
            WebDriverWait(self.driver, self.timeout).until(
                EC.presence_of_element_located((By.TAG_NAME, "table"))
            )
            
            time.sleep(self.delay)
            soup = BeautifulSoup(self.driver.page_source, "html.parser")
            return soup
        except Exception as e:
            self.logger.error(f"Error fetching year {year} via Selenium: {e}")
            return None

    def is_team_name(self, name: str) -> bool:
        """Check if a name looks like a team rather than a player"""
        team_words = ['yankees', 'red sox', 'tigers', 'white sox', 'athletics', 'orioles', 
                     'angels', 'rangers', 'mariners', 'astros', 'twins', 'royals', 'indians', 
                     'guardians', 'rays', 'brewers', 'cubs', 'pirates', 'cardinals', 'reds']
        return any(word in name.lower() for word in team_words) or len(name.split()) > 2

    def parse_tables(self, soup: BeautifulSoup, year: int):
        tables = soup.find_all("table")
        self.logger.info(f"Found {len(tables)} tables for year {year}")
        
        for i, table in enumerate(tables):
            table_text = table.get_text().lower()
            
            # Skip if table contains team standings data
            if ("won" in table_text and "lost" in table_text and 
                any(word in table_text for word in ['pct', 'gb', 'streak', 'home', 'road'])):
                self.parse_standings_table(table, year)
            # Individual hitting stats
            elif (any(keyword in table_text for keyword in ["batting average", "home run", "rbi", "hits", "doubles"]) and
                  not any(word in table_text for word in ['team', 'club', 'league'])):
                self.parse_hitting_table(table, year)
            # Individual pitching stats  
            elif (any(keyword in table_text for keyword in ["era", "strikeouts", "pitcher", "complete games"]) and
                  not any(word in table_text for word in ['team', 'club', 'league']) and
                  "won" not in table_text):
                self.parse_pitching_table(table, year)

    def parse_hitting_table(self, table, year: int):
        rows = table.find_all("tr")
        self.logger.info(f"Parsing hitting table with {len(rows)} rows for year {year}")
        
        for row in rows[1:]:  # Skip header
            cells = row.find_all(["td", "th"])
            if len(cells) < 3:
                continue
            
            try:
                values = [c.get_text(strip=True) for c in cells]
                
                # Skip if first column looks like a team name
                if self.is_team_name(values[0]):
                    continue
                
                player_name = None
                stat_value = None
                team = ""
                
                # Look for player name (usually in first column)
                if values and re.match(r"^[A-Z][a-z]+(\s[A-Z][a-z]*)*$", values[0]):
                    player_name = values[0]
                    team = values[1] if len(values) > 1 else ""
                    
                    # Look for numeric values
                    for val in values[2:]:
                        if re.match(r"^\d+$", val):  # Whole numbers (HR, RBI)
                            stat_value = int(val)
                            break
                        elif re.match(r"^\.\d{3}$", val):  # Batting average
                            stat_value = float(val)
                            break
                
                if player_name and stat_value is not None:
                    self.hitting_data.append({
                        "year": year,
                        "player_name": player_name,
                        "team": team,
                        "stat_category": self.determine_hitting_stat(values),
                        "stat_value": stat_value,
                    })
            except Exception as e:
                self.logger.debug(f"Error parsing hitting row: {e}")
                continue

    def determine_hitting_stat(self, values: List[str]) -> str:
        """Determine statistic type by context"""
        text = " ".join(values).lower()
        if "home run" in text or " hr " in text:
            return "Home Runs"
        elif "rbi" in text:
            return "RBI"
        elif "batting" in text or " avg " in text:
            return "Batting Average"
        elif "double" in text:
            return "Doubles"
        elif "triple" in text:
            return "Triples"
        elif "stolen" in text or "steal" in text:
            return "Stolen Bases"
        elif "run" in text and "home" not in text:
            return "Runs"
        elif "hit" in text:
            return "Hits"
        elif "walk" in text or "base on ball" in text:
            return "Walks"
        return "Unknown Hitting Stat"

    def parse_pitching_table(self, table, year: int):
        rows = table.find_all("tr")
        self.logger.info(f"Parsing pitching table with {len(rows)} rows for year {year}")
        
        for row in rows[1:]:
            cells = row.find_all(["td", "th"])
            if len(cells) < 3:
                continue
                
            try:
                values = [c.get_text(strip=True) for c in cells]
                
                # Skip if first column looks like a team name
                if self.is_team_name(values[0]):
                    continue
                
                player_name = None
                stat_value = None
                team = ""
                
                if values and re.match(r"^[A-Z][a-z]+(\s[A-Z][a-z]*)*$", values[0]):
                    player_name = values[0]
                    team = values[1] if len(values) > 1 else ""
                    
                    for val in values[2:]:
                        if re.match(r"^\d+$", val):  # Wins, Strikeouts
                            stat_value = int(val)
                            break
                        elif re.match(r"^\d+\.\d{2}$", val):  # ERA
                            stat_value = float(val)
                            break
                
                if player_name and stat_value is not None:
                    self.pitching_data.append({
                        "year": year,
                        "player_name": player_name,
                        "team": team,
                        "stat_category": self.determine_pitching_stat(values),
                        "stat_value": stat_value,
                    })
            except Exception as e:
                self.logger.debug(f"Error parsing pitching row: {e}")
                continue

    def determine_pitching_stat(self, values: List[str]) -> str:
        text = " ".join(values).lower()
        if "era" in text:
            return "ERA"
        elif "win" in text and "loss" not in text:
            return "Wins"
        elif "loss" in text:
            return "Losses"
        elif "strikeout" in text or " so " in text:
            return "Strikeouts"
        elif "save" in text:
            return "Saves"
        elif "complete" in text:
            return "Complete Games"
        elif "shutout" in text:
            return "Shutouts"
        return "Unknown Pitching Stat"

    def parse_standings_table(self, table, year: int):
        rows = table.find_all("tr")
        self.logger.info(f"Parsing standings table with {len(rows)} rows for year {year}")
        
        for row in rows:
            cells = row.find_all(["td", "th"])
            if len(cells) < 3:
                continue
                
            try:
                values = [c.get_text(strip=True) for c in cells]
                
                # Skip header rows
                if (not values[0] or 
                    any(word in values[0].lower() for word in ['team', 'club', 'division', 'league']) or
                    values[0].lower() in ['w', 'l', 'pct', 'gb']):
                    continue
                
                team_name = values[0]
                wins = 0
                losses = 0
                
                # Look for wins and losses (should be consecutive numbers)
                for i in range(1, min(len(values), 5)):
                    if values[i].isdigit() and int(values[i]) > 30:  # Reasonable win/loss count
                        if wins == 0:
                            wins = int(values[i])
                        elif losses == 0 and i+1 < len(values) and values[i+1].isdigit():
                            losses = int(values[i+1])
                            break
                
                # Only save if we have valid season data
                if wins > 30 and losses > 30 and wins + losses >= 140:  # Full season
                    win_pct = round(wins / (wins + losses), 3)
                    
                    self.standings_data.append({
                        "year": year,
                        "team_name": team_name,
                        "wins": wins,
                        "losses": losses,
                        "win_pct": win_pct,
                    })
                    self.logger.info(f"Added standings: {team_name} {wins}-{losses}")
                        
            except Exception as e:
                self.logger.debug(f"Error parsing standings row: {e}")
                continue

    def parse_notable_events(self, soup: BeautifulSoup, year: int):
        # Look for paragraphs with events
        paragraphs = soup.find_all("p")
        event_count = 0
        
        for p in paragraphs:
            text = p.get_text(strip=True)
            if not text or len(text) < 20:
                continue
                
            if any(kw in text.lower() for kw in ["no-hitter", "world series", "retire", "record", "death", "rookie", "debut"]):
                self.events_data.append({
                    "year": year,
                    "description": text,
                    "event_type": self.detect_event_type(text),
                    "participants": self.extract_names(text),
                    "significance": self.summarize_significance(text),
                })
                event_count += 1
        
        self.logger.info(f"Found {event_count} notable events for year {year}")

    def detect_event_type(self, text: str) -> str:
        text_lower = text.lower()
        if "no-hitter" in text_lower:
            return "No-Hitter"
        elif "world series" in text_lower:
            return "World Series"
        elif "rookie" in text_lower or "debut" in text_lower:
            return "Debut"
        elif "record" in text_lower:
            return "Record"
        elif "retire" in text_lower:
            return "Retirement"
        elif "death" in text_lower:
            return "Death"
        return "Other"

    def extract_names(self, text: str) -> str:
        # Improved name extraction
        names = re.findall(r"[A-Z][a-z]+\s+[A-Z][a-z]+(?:\s+[A-Z][a-z]+)?", text)
        return ", ".join(set(names[:5])) if names else ""  # Limit quantity

    def summarize_significance(self, text: str) -> str:
        return text[:100] + "..." if len(text) > 100 else text

    def scrape_year(self, year: int):
        # First try requests (faster)
        soup = self.fetch_year_page_requests(year)
        
        # If failed, use Selenium
        if not soup:
            soup = self.fetch_year_page(year)
            
        if soup:
            self.parse_tables(soup, year)
            self.parse_notable_events(soup, year)
            self.logger.info(f"Successfully parsed all data for {year}")
        else:
            self.logger.error(f"Failed to fetch data for {year}")

    def save_to_csv(self):
        os.makedirs("data/raw", exist_ok=True)
        
        # Save data with verification
        if self.hitting_data:
            pd.DataFrame(self.hitting_data).to_csv("data/raw/yearly_hitting_leaders.csv", index=False)
            self.logger.info(f"Saved {len(self.hitting_data)} hitting records")
        
        if self.pitching_data:
            pd.DataFrame(self.pitching_data).to_csv("data/raw/yearly_pitching_leaders.csv", index=False)
            self.logger.info(f"Saved {len(self.pitching_data)} pitching records")
        
        if self.standings_data:
            pd.DataFrame(self.standings_data).to_csv("data/raw/team_standings.csv", index=False)
            self.logger.info(f"Saved {len(self.standings_data)} standings records")
        else:
            self.logger.warning("No standings data found - check table detection logic")
        
        if self.events_data:
            pd.DataFrame(self.events_data).to_csv("data/raw/notable_events.csv", index=False)
            self.logger.info(f"Saved {len(self.events_data)} event records")

    def run(self, years: List[int]):
        self.logger.info(f"Starting scraping for {len(years)} years: {years}")
        
        for i, year in enumerate(years, 1):
            self.logger.info(f"Processing year {year} ({i}/{len(years)})")
            self.scrape_year(year)
            
            # Progressive delay on errors
            if i < len(years):
                time.sleep(self.delay * 2)  # Increased pause between years
        
        self.save_to_csv()
        self.logger.info("Scraping completed successfully!")


if __name__ == "__main__":
    print("MLB Historical Scraper — Significant Years & Notable Events")
    print("Starting enhanced version with improved error handling...")
    
    scraper = HistoricalMLBScraper(delay=4.0, timeout=45)  # Increased timeouts
    scraper.run(SIGNIFICANT_YEARS)