import sqlite3
import pandas as pd
import os

def create_database_from_csv():
    """Create SQLite database from cleaned CSV files"""
    
    print("Creating database from CSV files...")
    
    # Paths
    data_dir = 'data/cleaned'
    if not os.path.exists(data_dir):
        data_dir = 'data/raw'
        print(f"Using raw data from {data_dir}")
    else:
        print(f"Using cleaned data from {data_dir}")
    
    db_path = 'data/mlb_database.db'
    
    # Remove existing database
    if os.path.exists(db_path):
        os.remove(db_path)
        print("Removed existing database")
    
    # Create new database
    conn = sqlite3.connect(db_path)
    
    try:
        # Load and import standings
        suffix = "_cleaned" if data_dir == 'data/cleaned' else ""
        
        standings_df = pd.read_csv(f'{data_dir}/team_standings{suffix}.csv')
        standings_df.to_sql('standings', conn, if_exists='replace', index=False)
        print(f"Imported {len(standings_df)} standings records")
        
        # Load and import hitting leaders
        hitting_df = pd.read_csv(f'{data_dir}/yearly_hitting_leaders{suffix}.csv')
        hitting_df.to_sql('hitting_leaders', conn, if_exists='replace', index=False)
        print(f"Imported {len(hitting_df)} hitting records")
        
        # Load and import pitching leaders
        pitching_df = pd.read_csv(f'{data_dir}/yearly_pitching_leaders{suffix}.csv')
        pitching_df.to_sql('pitching_leaders', conn, if_exists='replace', index=False)
        print(f"Imported {len(pitching_df)} pitching records")
        
        # Load and import events
        events_df = pd.read_csv(f'{data_dir}/notable_events{suffix}.csv')
        events_df.to_sql('notable_events', conn, if_exists='replace', index=False)
        print(f"Imported {len(events_df)} event records")
        
        # Create indexes for better performance
        indexes = [
            "CREATE INDEX idx_standings_year ON standings(year);",
            "CREATE INDEX idx_hitting_year ON hitting_leaders(year);",
            "CREATE INDEX idx_pitching_year ON pitching_leaders(year);",
            "CREATE INDEX idx_events_year ON notable_events(year);",
            "CREATE INDEX idx_hitting_category ON hitting_leaders(stat_category);",
            "CREATE INDEX idx_pitching_category ON pitching_leaders(stat_category);",
            "CREATE INDEX idx_events_type ON notable_events(event_type);"
        ]
        
        for index_sql in indexes:
            conn.execute(index_sql)
        
        conn.commit()
        print("Created database indexes")
        
        print(f"\nDatabase created successfully at: {db_path}")
        print("You can now run the dashboard: streamlit run src/dashboard.py")
        
    except Exception as e:
        print(f"Error creating database: {e}")
    
    finally:
        conn.close()

if __name__ == "__main__":
    create_database_from_csv()