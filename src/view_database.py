import sqlite3
import pandas as pd

def view_database():
    """View database contents using Python"""
    
    print("Baseball Database Viewer")
    print("=" * 50)
    
    try:
        conn = sqlite3.connect("database/baseball.db")
        
        # Show table info
        print("TABLES IN DATABASE:")
        cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()
        for table in tables:
            if table[0] != 'sqlite_sequence':  # Skip system table
                print(f"  - {table[0]}")
        
        print("\n" + "=" * 50)
        
        # Babe Ruth records
        print("BABE RUTH RECORDS:")
        query = "SELECT year, player_name, team, stat_category, stat_value FROM hitting_leaders WHERE player_name = 'Babe Ruth'"
        df = pd.read_sql_query(query, conn)
        if not df.empty:
            print(df.to_string(index=False))
        else:
            print("No Babe Ruth records found")
        
        print("\n" + "=" * 50)
        
        # Top home run hitters
        print("TOP HOME RUN HITTERS:")
        query = """
        SELECT year, player_name, team, stat_value as home_runs 
        FROM hitting_leaders 
        WHERE stat_category = 'Home Runs' 
        ORDER BY stat_value DESC 
        LIMIT 10
        """
        df = pd.read_sql_query(query, conn)
        if not df.empty:
            print(df.to_string(index=False))
        else:
            print("No home run records found")
        
        print("\n" + "=" * 50)
        
        # Team standings
        print("TEAM STANDINGS:")
        query = "SELECT year, team_name, wins, losses, win_pct FROM team_standings ORDER BY year DESC, wins DESC"
        df = pd.read_sql_query(query, conn)
        if not df.empty:
            print(df.to_string(index=False))
        else:
            print("No team standings found")
        
        print("\n" + "=" * 50)
        
        # Database stats
        print("DATABASE STATISTICS:")
        stats_queries = [
            ("Hitting records", "SELECT COUNT(*) FROM hitting_leaders"),
            ("Pitching records", "SELECT COUNT(*) FROM pitching_leaders"),
            ("Team standings", "SELECT COUNT(*) FROM team_standings"),
            ("Quality issues logged", "SELECT COUNT(*) FROM data_quality_log")
        ]
        
        for name, query in stats_queries:
            cursor = conn.execute(query)
            result = cursor.fetchone()[0]
            print(f"  {name}: {result}")
        
        # Year range
        cursor = conn.execute("SELECT MIN(year), MAX(year) FROM hitting_leaders WHERE year IS NOT NULL")
        result = cursor.fetchone()
        if result and result[0]:
            print(f"  Year range: {result[0]}-{result[1]}")
        
        conn.close()
        
        print("\n" + "=" * 50)
        print("Database view complete!")
        
    except Exception as e:
        print(f"Error viewing database: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    view_database()