import sqlite3
import pandas as pd
import argparse
import sys
from typing import List, Dict

class BaseballQueryTool:
    """
    Command-line tool for querying baseball database with joins
    """
    
    def __init__(self, db_path: str = "database/baseball.db"):
        """Initialize query tool"""
        self.db_path = db_path
        try:
            self.conn = sqlite3.connect(db_path)
            print(f"Connected to database: {db_path}")
        except sqlite3.Error as e:
            print(f"Error connecting to database: {e}")
            sys.exit(1)
    
    def show_tables(self):
        """Show all tables in database"""
        print("\nTables in database:")
        print("=" * 40)
        
        cursor = self.conn.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
        tables = cursor.fetchall()
        
        for table in tables:
            if table[0] != 'sqlite_sequence':  # Skip system table
                print(f"- {table[0]}")
                
                # Show record count
                count_cursor = self.conn.execute(f"SELECT COUNT(*) FROM {table[0]}")
                count = count_cursor.fetchone()[0]
                print(f"  Records: {count}")
        print()
    
    def show_schema(self, table_name: str):
        """Show schema for specific table"""
        print(f"\nSchema for table: {table_name}")
        print("=" * 40)
        
        try:
            cursor = self.conn.execute(f"PRAGMA table_info({table_name})")
            columns = cursor.fetchall()
            
            for col in columns:
                col_name, col_type, not_null, default, primary_key = col[1], col[2], col[3], col[4], col[5]
                pk_flag = " (PRIMARY KEY)" if primary_key else ""
                null_flag = " NOT NULL" if not_null else ""
                print(f"  {col_name}: {col_type}{null_flag}{pk_flag}")
                
        except sqlite3.Error as e:
            print(f"Error getting schema: {e}")
        print()
    
    def run_query(self, query: str, limit: int = 10):
        """Run custom SQL query"""
        print(f"\nExecuting query:")
        print(f"SQL: {query}")
        print("=" * 60)
        
        try:
            cursor = self.conn.execute(query)
            results = cursor.fetchall()
            columns = [description[0] for description in cursor.description]
            
            if not results:
                print("No results found.")
                return
            
            # Print header
            header = " | ".join(f"{col:15}" for col in columns)
            print(header)
            print("-" * len(header))
            
            # Print results (limited)
            for i, row in enumerate(results[:limit]):
                formatted_row = []
                for item in row:
                    if isinstance(item, float):
                        formatted_row.append(f"{item:15.3f}")
                    elif item is None:
                        formatted_row.append(f"{'NULL':15}")
                    else:
                        formatted_row.append(f"{str(item)[:15]:15}")
                print(" | ".join(formatted_row))
            
            if len(results) > limit:
                print(f"... and {len(results) - limit} more rows")
                
            print(f"\nTotal rows: {len(results)}")
            
        except sqlite3.Error as e:
            print(f"SQL Error: {e}")
        print()
    
    def predefined_queries(self):
        """Show menu of predefined queries with joins"""
        print("\nPredefined Queries (with JOINS):")
        print("=" * 40)
        
        queries = {
            "1": {
                "name": "Top home run hitters with team performance",
                "sql": """
                SELECT 
                    h.year,
                    h.player_name,
                    h.team,
                    h.stat_value as home_runs,
                    t.wins,
                    t.losses,
                    t.win_pct
                FROM hitting_leaders h
                LEFT JOIN team_standings t ON h.year = t.year 
                    AND (h.team LIKE '%' || REPLACE(t.team_name, ' ', '%') || '%' 
                         OR t.team_name LIKE '%' || REPLACE(h.team, ' ', '%') || '%')
                WHERE h.stat_category = 'Home Runs'
                ORDER BY h.stat_value DESC
                """
            },
            "2": {
                "name": "Yankees players across all years with team standings",
                "sql": """
                SELECT DISTINCT
                    h.year,
                    h.player_name,
                    h.stat_category,
                    h.stat_value,
                    t.wins,
                    t.losses,
                    t.win_pct
                FROM hitting_leaders h
                JOIN team_standings t ON h.year = t.year
                WHERE (h.team LIKE '%Yankees%' OR t.team_name LIKE '%Yankees%')
                  AND h.team LIKE '%Yankees%'
                ORDER BY h.year DESC, h.stat_value DESC
                """
            },
            "3": {
                "name": "Best hitting and pitching performances by year",
                "sql": """
                SELECT 
                    h.year,
                    h.player_name as best_hitter,
                    h.stat_category as hitting_stat,
                    h.stat_value as hitting_value,
                    p.player_name as best_pitcher,
                    p.stat_category as pitching_stat,
                    p.stat_value as pitching_value
                FROM 
                    (SELECT year, player_name, stat_category, stat_value,
                            ROW_NUMBER() OVER (PARTITION BY year ORDER BY stat_value DESC) as rn
                     FROM hitting_leaders WHERE stat_category = 'Home Runs') h
                JOIN 
                    (SELECT year, player_name, stat_category, stat_value,
                            ROW_NUMBER() OVER (PARTITION BY year ORDER BY stat_value ASC) as rn
                     FROM pitching_leaders WHERE stat_category = 'ERA') p
                ON h.year = p.year AND h.rn = 1 AND p.rn = 1
                ORDER BY h.year DESC
                """
            },
            "4": {
                "name": "Data quality analysis across tables",
                "sql": """
                SELECT 
                    'hitting_leaders' as table_name,
                    quality_level,
                    COUNT(*) as record_count,
                    AVG(quality_score) as avg_quality_score
                FROM hitting_leaders
                GROUP BY quality_level
                UNION ALL
                SELECT 
                    'pitching_leaders' as table_name,
                    quality_level,
                    COUNT(*) as record_count,
                    AVG(quality_score) as avg_quality_score
                FROM pitching_leaders
                GROUP BY quality_level
                ORDER BY table_name, avg_quality_score DESC
                """
            },
            "5": {
                "name": "Team performance correlation with star players",
                "sql": """
                SELECT 
                    t.year,
                    t.team_name,
                    t.wins,
                    t.win_pct,
                    COUNT(h.player_name) as star_players,
                    AVG(h.stat_value) as avg_stat_value
                FROM team_standings t
                LEFT JOIN hitting_leaders h ON t.year = h.year
                    AND (h.team LIKE '%' || REPLACE(t.team_name, ' ', '%') || '%'
                         OR t.team_name LIKE '%' || REPLACE(h.team, ' ', '%') || '%')
                    AND h.stat_value > 40  -- High performance threshold
                GROUP BY t.year, t.team_name, t.wins, t.win_pct
                ORDER BY t.win_pct DESC
                """
            }
        }
        
        for key, query in queries.items():
            print(f"{key}. {query['name']}")
        
        print("\n0. Custom query")
        print("q. Quit")
        
        return queries
    
    def interactive_mode(self):
        """Interactive command-line interface"""
        print("\nBaseball Database Query Tool")
        print("=" * 40)
        print("Commands:")
        print("  tables    - Show all tables")
        print("  schema    - Show table schema")
        print("  queries   - Predefined queries with joins")
        print("  sql       - Run custom SQL")
        print("  quit      - Exit")
        print()
        
        while True:
            try:
                command = input("baseball_db> ").strip().lower()
                
                if command in ['quit', 'exit', 'q']:
                    break
                elif command == 'tables':
                    self.show_tables()
                elif command == 'schema':
                    table = input("Enter table name: ").strip()
                    self.show_schema(table)
                elif command == 'queries':
                    queries = self.predefined_queries()
                    choice = input("\nSelect query (1-5, 0 for custom, q to quit): ").strip()
                    
                    if choice == 'q':
                        continue
                    elif choice == '0':
                        sql = input("Enter SQL query: ").strip()
                        if sql:
                            self.run_query(sql)
                    elif choice in queries:
                        self.run_query(queries[choice]['sql'])
                    else:
                        print("Invalid choice")
                elif command == 'sql':
                    sql = input("Enter SQL query: ").strip()
                    if sql:
                        self.run_query(sql)
                elif command == 'help':
                    print("Available commands: tables, schema, queries, sql, quit")
                else:
                    print(f"Unknown command: {command}. Type 'help' for available commands.")
                    
            except KeyboardInterrupt:
                print("\nExiting...")
                break
            except Exception as e:
                print(f"Error: {e}")
    
    def close(self):
        """Close database connection"""
        if self.conn:
            self.conn.close()
            print("Database connection closed.")

def main():
    """Main function for command-line usage"""
    parser = argparse.ArgumentParser(description="Baseball Database Query Tool")
    parser.add_argument("--db", default="database/baseball.db", help="Database file path")
    parser.add_argument("--query", "-q", help="SQL query to execute")
    parser.add_argument("--tables", action="store_true", help="Show all tables")
    parser.add_argument("--interactive", "-i", action="store_true", help="Interactive mode")
    
    args = parser.parse_args()
    
    # Create query tool
    query_tool = BaseballQueryTool(args.db)
    
    try:
        if args.tables:
            query_tool.show_tables()
        elif args.query:
            query_tool.run_query(args.query)
        elif args.interactive:
            query_tool.interactive_mode()
        else:
            # Default: show sample queries
            print("Baseball Database Query Tool")
            print("Usage examples:")
            print("  python src/db_query.py --tables")
            print("  python src/db_query.py --interactive")
            print('  python src/db_query.py --query "SELECT * FROM hitting_leaders LIMIT 5"')
            print()
            query_tool.show_tables()
            
    finally:
        query_tool.close()

if __name__ == "__main__":
    main()