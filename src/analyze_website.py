# src/analyze_website_fallback.py
import requests
from bs4 import BeautifulSoup
import re
import time

def analyze_with_requests():
    """
    Analyze Baseball Almanac using only requests and BeautifulSoup
    This avoids ChromeDriver version issues
    """
    print("Starting Baseball Almanac analysis with requests...")
    
    # Target URLs from Baseball Almanac
    target_urls = {
        "main_page": "https://www.baseball-almanac.com/yearmenu.shtml",
        "sample_year_1927": "https://www.baseball-almanac.com/yearly/yr1927a.shtml",
        "sample_year_2000": "https://www.baseball-almanac.com/yearly/yr2000a.shtml"
    }
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    
    analysis_results = {}
    
    # Analyze main year menu page
    print(f"\nAnalyzing main page: {target_urls['main_page']}")
    
    try:
        response = requests.get(target_urls['main_page'], headers=headers, timeout=10)
        
        if response.status_code == 200:
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Extract year links using regex on the page source
            page_text = response.text
            year_pattern = r'yr(\d{4})a\.shtml'
            year_matches = re.findall(year_pattern, page_text)
            
            # Remove duplicates and sort
            unique_years = sorted(list(set(year_matches)))
            
            print(f"Found {len(unique_years)} unique year links")
            if unique_years:
                print(f"Year range: {unique_years[0]} - {unique_years[-1]}")
                print(f"Sample years: {unique_years[:5]} ... {unique_years[-5:]}")
            
            analysis_results['main_page'] = {
                "url": target_urls['main_page'],
                "title": soup.title.string if soup.title else "No title",
                "year_links_found": len(unique_years),
                "year_range": f"{unique_years[0]}-{unique_years[-1]}" if unique_years else "None",
                "years_available": unique_years,
                "accessible": True
            }
            
        else:
            print(f"Failed to access main page: Status {response.status_code}")
            return None
            
    except Exception as e:
        print(f"Error accessing main page: {e}")
        return None
    
    # Analyze sample year pages
    for page_name, url in [("sample_year_1927", target_urls['sample_year_1927']), 
                          ("sample_year_2000", target_urls['sample_year_2000'])]:
        print(f"\nAnalyzing {page_name}: {url}")
        
        try:
            time.sleep(1)  # Be respectful to the server
            response = requests.get(url, headers=headers, timeout=10)
            
            if response.status_code == 200:
                soup = BeautifulSoup(response.content, 'html.parser')
                page_text = response.text.lower()
                
                # Get page title
                title = soup.title.string if soup.title else "No title"
                
                # Count tables (main data structure)
                tables = soup.find_all('table')
                table_count = len(tables)
                
                # Look for specific data indicators
                hitting_mentions = len(re.findall(r'hitting.*leader', page_text))
                pitching_mentions = len(re.findall(r'pitching.*leader', page_text))
                
                # Look for statistical patterns
                batting_averages = len(re.findall(r'\.\d{3}', response.text))  # .xxx format
                era_stats = len(re.findall(r'\d\.\d{2}', response.text))  # ERA format
                
                # Look for team names and player names
                teams_found = 0
                for team_name in ['Yankees', 'Red Sox', 'Cardinals', 'Giants', 'Dodgers', 'Cubs']:
                    if team_name.lower() in page_text:
                        teams_found += 1
                
                # Count potential player entries (capitalized names)
                player_pattern = r'[A-Z][a-z]+ [A-Z][a-z]+'
                potential_players = len(set(re.findall(player_pattern, response.text)))
                
                analysis_results[page_name] = {
                    "url": url,
                    "title": title,
                    "tables": table_count,
                    "hitting_mentions": hitting_mentions,
                    "pitching_mentions": pitching_mentions,
                    "batting_averages": batting_averages,
                    "era_stats": era_stats,
                    "teams_found": teams_found,
                    "potential_players": potential_players,
                    "accessible": True,
                    "data_quality_score": table_count + hitting_mentions + pitching_mentions + teams_found
                }
                
                print(f"Title: {title}")
                print(f"Tables found: {table_count}")
                print(f"Hitting leader mentions: {hitting_mentions}")
                print(f"Pitching leader mentions: {pitching_mentions}")
                print(f"Batting averages found: {batting_averages}")
                print(f"ERA stats found: {era_stats}")
                print(f"Team names found: {teams_found}")
                print(f"Potential player names: {potential_players}")
                print(f"Data quality score: {analysis_results[page_name]['data_quality_score']}")
                
            else:
                print(f"Failed to access {page_name}: Status {response.status_code}")
                analysis_results[page_name] = {
                    "url": url,
                    "accessible": False,
                    "status_code": response.status_code
                }
                
        except Exception as e:
            print(f"Error analyzing {page_name}: {e}")
            analysis_results[page_name] = {
                "url": url,
                "accessible": False,
                "error": str(e)
            }
    
    return analysis_results

def generate_detailed_strategy(analysis_results):
    """
    Generate detailed scraping strategy based on analysis
    """
    print("\n" + "="*70)
    print("DETAILED SCRAPING STRATEGY FOR BASEBALL ALMANAC")
    print("="*70)
    
    if not analysis_results:
        print("No analysis data available")
        return
    
    # Display comprehensive analysis
    print("\nWEBSITE ANALYSIS SUMMARY:")
    main_page = analysis_results.get('main_page', {})
    if main_page.get('accessible'):
        print(f"\nMAIN PAGE ANALYSIS:")
        print(f"  Total years available: {main_page['year_links_found']}")
        print(f"  Coverage period: {main_page['year_range']}")
        print(f"  URL pattern: https://www.baseball-almanac.com/yearly/yr[YEAR]a.shtml")
    
    # Analyze sample pages
    sample_pages = [key for key in analysis_results.keys() if key.startswith('sample_year')]
    
    if sample_pages:
        print(f"\nSAMPLE PAGES ANALYSIS:")
        total_score = 0
        accessible_pages = 0
        
        for page_name in sample_pages:
            page_data = analysis_results[page_name]
            if page_data.get('accessible'):
                accessible_pages += 1
                score = page_data.get('data_quality_score', 0)
                total_score += score
                
                print(f"\n  {page_name.upper()}:")
                print(f"    Data tables: {page_data.get('tables', 0)}")
                print(f"    Statistical data points: {page_data.get('batting_averages', 0) + page_data.get('era_stats', 0)}")
                print(f"    Team references: {page_data.get('teams_found', 0)}")
                print(f"    Player entries: {page_data.get('potential_players', 0)}")
        
        if accessible_pages > 0:
            avg_score = total_score / accessible_pages
            print(f"\n  OVERALL ASSESSMENT:")
            print(f"    Average data quality score: {avg_score:.1f}/10")
            print(f"    Data richness: {'Excellent' if avg_score > 7 else 'Good' if avg_score > 4 else 'Moderate'}")
    
    # Generate implementation strategy
    print("\n" + "="*50)
    print("IMPLEMENTATION STRATEGY")
    print("="*50)
    
    years_available = main_page.get('years_available', [])
    if years_available:
        total_years = len(years_available)
        recent_years = [y for y in years_available if int(y) >= 2000]
        historical_years = [y for y in years_available if int(y) < 2000]
        
        print(f"\nDATA SCOPE:")
        print(f"  Total years to scrape: {total_years}")
        print(f"  Historical years (pre-2000): {len(historical_years)}")
        print(f"  Modern era (2000+): {len(recent_years)}")
        
        print(f"\nRECOMMENDED SCRAPING PHASES:")
        print(f"  Phase 1: Test with recent years ({recent_years[-5:]})")
        print(f"  Phase 2: Historical data ({historical_years[0]} - {historical_years[-1]})")
        print(f"  Phase 3: Complete modern era ({recent_years[0]} - {recent_years[-1]})")
    
    print(f"\nTECHNICAL IMPLEMENTATION:")
    print(f"  Method: requests + BeautifulSoup (avoid ChromeDriver issues)")
    print(f"  Rate limiting: 1-2 seconds between requests")
    print(f"  Error handling: Retry failed requests, log missing years")
    print(f"  Data validation: Check for expected table structures")
    
    print(f"\nCSV OUTPUT FILES:")
    print(f"  1. yearly_hitting_leaders.csv")
    print(f"     Columns: year, rank, player_name, team, stat_category, stat_value")
    print(f"  2. yearly_pitching_leaders.csv")
    print(f"     Columns: year, rank, player_name, team, stat_category, stat_value")
    print(f"  3. team_standings.csv")
    print(f"     Columns: year, league, team_name, wins, losses, win_pct, finish_position")
    print(f"  4. notable_events.csv")
    print(f"     Columns: year, event_type, description, participants, significance")

if __name__ == "__main__":
    print("Baseball Almanac Analysis - Requests Method")
    print("="*60)
    print("This version avoids ChromeDriver compatibility issues")
    
    try:
        results = analyze_with_requests()
        
        if results:
            generate_detailed_strategy(results)
            print(f"\nSUCCESS: Analysis complete!")
            print(f"Ready to proceed with scraper implementation.")
        else:
            print(f"FAILED: Could not complete analysis")
            
    except Exception as e:
        print(f"Analysis failed with error: {e}")
    
    print(f"\nNext step: Create the web scraper using requests method!")