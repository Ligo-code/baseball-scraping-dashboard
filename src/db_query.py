import sqlite3
import pandas as pd
import os
import sys
from typing import List, Dict, Any

class MLBQueryProgram:
    def __init__(self, db_path: str = 'data/mlb_database.db'):
        self.db_path = db_path
        self.conn = None
        self.predefined_queries = {
            '1': {
                'name': 'Top Home Run Leaders by Year',
                'query': '''
                    SELECT h.year, h.player_name, h.team, h.stat_value as home_runs
                    FROM hitting_leaders h
                    WHERE h.stat_category = 'Home Runs'
                    ORDER BY h.year, h.stat_value DESC;
                '''
            },
            '2': {
                'name': 'ERA Leaders and Team Performance',
                'query': '''
                    SELECT p.year, p.player_name, p.team, p.stat_value as era,
                           ROUND(AVG(s.win_pct), 3) as avg_league_win_pct,
                           COUNT(s.team_name) as teams_that_year
                    FROM pitching_leaders p
                    JOIN standings s ON p.year = s.year 
                    WHERE p.stat_category = 'ERA'
                    GROUP BY p.year, p.player_name, p.team, p.stat_value
                    ORDER BY p.year, p.stat_value ASC;
                '''
            },
            '3': {
                'name': 'Notable Events by Type and Year',
                'query': '''
                    SELECT e.year, e.event_type, COUNT(*) as event_count,
                           ROUND(AVG(s.win_pct), 3) as avg_league_win_pct
                    FROM notable_events e
                    JOIN standings s ON e.year = s.year
                    GROUP BY e.year, e.event_type
                    HAVING event_count > 1
                    ORDER BY e.year, event_count DESC;
                '''
            },
            '4': {
                'name': 'Yankees Performance Across Years',
                'query': '''
                    SELECT s.year, s.wins, s.losses, s.win_pct,
                           (SELECT COUNT(*) FROM hitting_leaders h 
                            WHERE h.year = s.year AND h.team LIKE '%New York%') as hitting_leaders,
                           (SELECT COUNT(*) FROM pitching_leaders p 
                            WHERE p.year = s.year AND p.team LIKE '%New York%') as pitching_leaders,
                           (SELECT COUNT(*) FROM notable_events e 
                            WHERE e.year = s.year AND e.description LIKE '%Yankees%') as yankees_events
                    FROM standings s
                    WHERE s.team_name LIKE '%Yankees%'
                    ORDER BY s.year;
                '''
            },
            '5': {
                'name': 'Best Season Performances (100+ Wins)',
                'query': '''
                    SELECT s.year, s.team_name, s.wins, s.losses, s.win_pct,
                           (SELECT COUNT(*) FROM notable_events e 
                            WHERE e.year = s.year) as total_events_that_year
                    FROM standings s
                    WHERE s.wins >= 100
                    ORDER BY s.wins DESC, s.year;
                '''
            },
            '6': {
                'name': 'World Series and Championship Events',
                'query': '''
                    SELECT e.year, e.event_type, 
                           SUBSTR(e.description, 1, 100) || '...' as event_summary,
                           COUNT(s.team_name) as teams_in_league
                    FROM notable_events e
                    JOIN standings s ON e.year = s.year
                    WHERE e.event_type = 'World Series' 
                       OR e.description LIKE '%World Series%'
                       OR e.description LIKE '%championship%'
                    GROUP BY e.year, e.event_type, e.description
                    ORDER BY e.year;
                '''
            },
            '7': {
                'name': 'Player Records and No-Hitters',
                'query': '''
                    SELECT e.year, e.event_type, 
                           SUBSTR(e.description, 1, 80) || '...' as event_summary
                    FROM notable_events e
                    WHERE e.event_type IN ('Record', 'No-Hitter', 'Award')
                    ORDER BY e.year, e.event_type;
                '''
            },
            '8': {
                'name': 'Home Run Kings by Decade',
                'query': '''
                    SELECT h.year, h.player_name, h.team, h.stat_value as home_runs,
                           CASE 
                               WHEN h.year < 1950 THEN '1920s-1940s'
                               WHEN h.year < 1970 THEN '1950s-1960s' 
                               WHEN h.year < 1990 THEN '1970s-1980s'
                               WHEN h.year < 2010 THEN '1990s-2000s'
                               ELSE '2010s-2020s'
                           END as era
                    FROM hitting_leaders h
                    WHERE h.stat_category = 'Home Runs'
                    ORDER BY h.stat_value DESC, h.year;
                '''
            },
            '9': {
                'name': 'Pitching Dominance: Strikeout Leaders',
                'query': '''
                    SELECT p.year, p.player_name, p.team, p.stat_value as strikeouts,
                           s.team_name, s.wins, s.losses
                    FROM pitching_leaders p
                    LEFT JOIN standings s ON p.year = s.year 
                        AND (s.team_name LIKE '%' || p.team || '%' 
                             OR p.team LIKE '%' || SUBSTR(s.team_name, INSTR(s.team_name, ' ') + 1) || '%')
                    WHERE p.stat_category = 'Strikeouts'
                    ORDER BY p.stat_value DESC, p.year;
                '''
            },
            '10': {
                'name': 'Team Dynasties: Multiple 100-Win Seasons',
                'query': '''
                    SELECT team_name, 
                           COUNT(*) as seasons_100_plus_wins,
                           AVG(wins) as avg_wins,
                           MIN(year) as first_great_season,
                           MAX(year) as last_great_season
                    FROM standings 
                    WHERE wins >= 100
                    GROUP BY team_name
                    HAVING COUNT(*) >= 1
                    ORDER BY seasons_100_plus_wins DESC, avg_wins DESC;
                '''
            }
        }
    
    def connect(self):
        """Connect to the database"""
        if not os.path.exists(self.db_path):
            print(f"Database not found: {self.db_path}")
            print("Please run the database import script first.")
            return False
        
        try:
            self.conn = sqlite3.connect(self.db_path)
            print(f"Connected to database: {self.db_path}")
            return True
        except sqlite3.Error as e:
            print(f"Error connecting to database: {e}")
            return False
    
    def disconnect(self):
        """Close database connection"""
        if self.conn:
            self.conn.close()
            print("Database connection closed.")
    
    def execute_query(self, query: str, params: tuple = ()) -> pd.DataFrame:
        """Execute a SQL query and return results as DataFrame"""
        try:
            df = pd.read_sql_query(query, self.conn, params=params)
            return df
        except sqlite3.Error as e:
            print(f"SQL Error: {e}")
            return pd.DataFrame()
        except Exception as e:
            print(f"Error executing query: {e}")
            return pd.DataFrame()
    
    def display_results(self, df: pd.DataFrame, max_rows: int = 20):
        """Display query results in a formatted way"""
        if df.empty:
            print("No results found.")
            return
        
        print(f"\nQuery returned {len(df)} rows:")
        print("=" * 80)
        
        # Show first max_rows
        display_df = df.head(max_rows)
        
        # Format the output
        pd.set_option('display.max_columns', None)
        pd.set_option('display.width', None)
        pd.set_option('display.max_colwidth', 50)
        
        print(display_df.to_string(index=False))
        
        if len(df) > max_rows:
            print(f"\n... and {len(df) - max_rows} more rows")
        
        print("=" * 80)
    
    def show_predefined_queries(self):
        """Display available predefined queries"""
        print("\n" + "="*60)
        print("AVAILABLE PREDEFINED QUERIES")
        print("="*60)
        
        for key, query_info in self.predefined_queries.items():
            print(f"{key:2}. {query_info['name']}")
        
        print("11. Custom Query")
        print("12. Show Database Schema")
        print("13. Filter by Year")
        print(" 0. Exit")
        print("="*60)
    
    def run_predefined_query(self, query_key: str):
        """Run a predefined query"""
        if query_key in self.predefined_queries:
            query_info = self.predefined_queries[query_key]
            print(f"\nRunning: {query_info['name']}")
            print("-" * 40)
            
            df = self.execute_query(query_info['query'])
            self.display_results(df)
        else:
            print("Invalid query selection.")
    
    def run_custom_query(self):
        """Allow user to input and run custom SQL"""
        print("\nEnter your custom SQL query (type 'cancel' to return to main menu):")
        print("Example: SELECT * FROM standings WHERE year = 1927;")
        print("-" * 40)
        
        query_lines = []
        while True:
            try:
                line = input("SQL> " if not query_lines else "...> ")
                if line.lower().strip() == 'cancel':
                    return
                
                query_lines.append(line)
                
                # Check if query is complete (ends with semicolon)
                if line.strip().endswith(';'):
                    break
                    
            except KeyboardInterrupt:
                print("\nQuery cancelled.")
                return
        
        query = ' '.join(query_lines)
        
        if query.strip():
            print(f"\nExecuting query...")
            df = self.execute_query(query)
            self.display_results(df)
    
    def show_schema(self):
        """Display database schema information"""
        print("\n" + "="*60)
        print("DATABASE SCHEMA")
        print("="*60)
        
        # Get table information
        tables_query = "SELECT name FROM sqlite_master WHERE type='table';"
        tables_df = self.execute_query(tables_query)
        
        for table_name in tables_df['name']:
            print(f"\nTable: {table_name}")
            print("-" * 30)
            
            # Get column information
            pragma_query = f"PRAGMA table_info({table_name});"
            columns_df = self.execute_query(pragma_query)
            
            for _, row in columns_df.iterrows():
                nullable = "NOT NULL" if row['notnull'] else "NULL"
                pk = " (PRIMARY KEY)" if row['pk'] else ""
                print(f"  {row['name']}: {row['type']} {nullable}{pk}")
            
            # Get row count
            count_query = f"SELECT COUNT(*) as count FROM {table_name};"
            count_df = self.execute_query(count_query)
            if not count_df.empty:
                print(f"  Records: {count_df.iloc[0]['count']}")
    
    def filter_by_year(self):
        """Interactive year filtering"""
        try:
            year = int(input("Enter year to filter by: "))
            
            print(f"\nComplete Data for {year}:")
            print("="*50)
            
            # Standings for that year
            standings_query = "SELECT team_name, wins, losses, win_pct FROM standings WHERE year = ? ORDER BY wins DESC;"
            standings_df = self.execute_query(standings_query, (year,))
            if not standings_df.empty:
                print(f"\nTeam Standings:")
                self.display_results(standings_df)
            
            # Top hitting stats by category
            hitting_query = """
                SELECT stat_category, player_name, team, stat_value 
                FROM hitting_leaders 
                WHERE year = ? 
                ORDER BY stat_category, stat_value DESC;
            """
            hitting_df = self.execute_query(hitting_query, (year,))
            if not hitting_df.empty:
                print(f"\nHitting Leaders:")
                self.display_results(hitting_df)
            
            # Top pitching stats
            pitching_query = """
                SELECT stat_category, player_name, team, stat_value 
                FROM pitching_leaders 
                WHERE year = ? 
                ORDER BY stat_category, stat_value DESC;
            """
            pitching_df = self.execute_query(pitching_query, (year,))
            if not pitching_df.empty:
                print(f"\nPitching Leaders:")
                self.display_results(pitching_df)
            
            # Notable events
            events_query = "SELECT event_type, SUBSTR(description, 1, 100) || '...' as summary FROM notable_events WHERE year = ?;"
            events_df = self.execute_query(events_query, (year,))
            if not events_df.empty:
                print(f"\nNotable Events:")
                self.display_results(events_df)
                
        except ValueError:
            print("Please enter a valid year (number).")
        except Exception as e:
            print(f"Error: {e}")
    
    def interactive_menu(self):
        """Main interactive menu"""
        if not self.connect():
            return
        
        print("\n" + "="*60)
        print("MLB HISTORICAL DATA QUERY PROGRAM")
        print("="*60)
        print("Welcome! This program allows you to query MLB historical data.")
        print("Data includes team standings, player statistics, and notable events")
        print("from significant years in baseball history.")
        
        try:
            while True:
                self.show_predefined_queries()
                
                try:
                    choice = input("\nSelect an option (0-13): ").strip()
                    
                    if choice == '0':
                        print("Goodbye!")
                        break
                    elif choice in self.predefined_queries:
                        self.run_predefined_query(choice)
                    elif choice == '11':
                        self.run_custom_query()
                    elif choice == '12':
                        self.show_schema()
                    elif choice == '13':
                        self.filter_by_year()
                    else:
                        print("Invalid choice. Please select 0-13.")
                    
                    # Pause before showing menu again
                    input("\nPress Enter to continue...")
                    
                except KeyboardInterrupt:
                    print("\n\nExiting...")
                    break
                    
        finally:
            self.disconnect()
    
    def run_query_from_args(self):
        """Run queries from command line arguments"""
        if len(sys.argv) < 2:
            print("Usage: python db_query.py <query_number> or python db_query.py")
            print("Run without arguments for interactive mode.")
            return
        
        if not self.connect():
            return
        
        try:
            query_num = sys.argv[1]
            if query_num in self.predefined_queries:
                self.run_predefined_query(query_num)
            else:
                print(f"Invalid query number: {query_num}")
                print("Available queries: " + ", ".join(self.predefined_queries.keys()))
        finally:
            self.disconnect()

def main():
    """Main entry point"""
    query_program = MLBQueryProgram()
    
    # Check if command line arguments provided
    if len(sys.argv) > 1:
        query_program.run_query_from_args()
    else:
        query_program.interactive_menu()

if __name__ == "__main__":
    main()