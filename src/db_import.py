import sqlite3
import pandas as pd
import os
from typing import Dict, Any

class MLBDatabaseImporter:
    def __init__(self, db_path: str = 'data/mlb_database.db'):
        self.db_path = db_path
        self.conn = None
        
    def connect(self):
        """Create database connection"""
        try:
            # Create data directory if it doesn't exist
            os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
            
            self.conn = sqlite3.connect(self.db_path)
            # Disable foreign keys during import to avoid constraint issues
            self.conn.execute("PRAGMA foreign_keys = OFF")
            print(f"Connected to database: {self.db_path}")
            
        except sqlite3.Error as e:
            print(f"Error connecting to database: {e}")
            raise
    
    def create_tables(self):
        """Create all tables with proper data types and constraints"""
        
        # Drop existing tables to start fresh
        drop_tables = [
            "DROP TABLE IF EXISTS notable_events;",
            "DROP TABLE IF EXISTS pitching_leaders;", 
            "DROP TABLE IF EXISTS hitting_leaders;",
            "DROP TABLE IF EXISTS standings;"
        ]
        
        for drop_sql in drop_tables:
            self.conn.execute(drop_sql)
        
        # Team standings table (base table, no foreign keys)
        standings_sql = """
        CREATE TABLE standings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            year INTEGER NOT NULL,
            team_name TEXT NOT NULL,
            wins INTEGER NOT NULL CHECK (wins >= 0),
            losses INTEGER NOT NULL CHECK (losses >= 0),
            win_pct REAL NOT NULL CHECK (win_pct >= 0 AND win_pct <= 1),
            games_played INTEGER GENERATED ALWAYS AS (wins + losses) STORED,
            UNIQUE(year, team_name)
        );
        """
        
        # Hitting leaders table
        hitting_sql = """
        CREATE TABLE hitting_leaders (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            year INTEGER NOT NULL,
            player_name TEXT NOT NULL,
            team TEXT,
            stat_category TEXT NOT NULL,
            stat_value REAL NOT NULL
        );
        """
        
        # Pitching leaders table  
        pitching_sql = """
        CREATE TABLE pitching_leaders (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            year INTEGER NOT NULL,
            player_name TEXT NOT NULL,
            team TEXT,
            stat_category TEXT NOT NULL,
            stat_value REAL NOT NULL
        );
        """
        
        # Notable events table
        events_sql = """
        CREATE TABLE notable_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            year INTEGER NOT NULL,
            description TEXT NOT NULL,
            event_type TEXT NOT NULL,
            description_length INTEGER GENERATED ALWAYS AS (LENGTH(description)) STORED
        );
        """
        
        # Execute table creation in order
        tables = {
            'standings': standings_sql,
            'hitting_leaders': hitting_sql, 
            'pitching_leaders': pitching_sql,
            'notable_events': events_sql
        }
        
        for table_name, sql in tables.items():
            try:
                self.conn.execute(sql)
                print(f"Created table: {table_name}")
            except sqlite3.Error as e:
                print(f"Error creating table {table_name}: {e}")
                raise
        
        self.conn.commit()
    
    def validate_csv_file(self, file_path: str, required_columns: list) -> bool:
        """Validate CSV file exists and has required columns"""
        if not os.path.exists(file_path):
            print(f"File not found: {file_path}")
            return False
        
        try:
            df = pd.read_csv(file_path, nrows=1)
            missing_cols = set(required_columns) - set(df.columns)
            if missing_cols:
                print(f"Missing columns in {file_path}: {missing_cols}")
                return False
            return True
        except Exception as e:
            print(f"Error reading {file_path}: {e}")
            return False
    
    def import_standings(self, file_path: str = 'data/raw/team_standings.csv'):
        """Import team standings data"""
        required_cols = ['year', 'team_name', 'wins', 'losses', 'win_pct']
        
        if not self.validate_csv_file(file_path, required_cols):
            return False
        
        try:
            df = pd.read_csv(file_path)
            
            # Data validation and cleaning
            df = df.dropna(subset=required_cols)
            df = df[(df['wins'] >= 0) & (df['losses'] >= 0)]
            df = df[(df['win_pct'] >= 0) & (df['win_pct'] <= 1)]
            
            # Convert data types
            df['year'] = df['year'].astype(int)
            df['wins'] = df['wins'].astype(int)
            df['losses'] = df['losses'].astype(int)
            df['win_pct'] = df['win_pct'].astype(float)
            
            # Insert data using executemany for better performance
            insert_sql = """
            INSERT INTO standings (year, team_name, wins, losses, win_pct)
            VALUES (?, ?, ?, ?, ?)
            """
            
            data_tuples = [
                (row['year'], row['team_name'], row['wins'], row['losses'], row['win_pct'])
                for _, row in df.iterrows()
            ]
            
            self.conn.executemany(insert_sql, data_tuples)
            self.conn.commit()
            
            print(f"Imported {len(df)} standings records")
            return True
            
        except Exception as e:
            print(f"Error importing standings: {e}")
            self.conn.rollback()
            return False
    
    def import_hitting_leaders(self, file_path: str = 'data/raw/yearly_hitting_leaders.csv'):
        """Import hitting leaders data"""
        required_cols = ['year', 'player_name', 'stat_category', 'stat_value']
        
        if not self.validate_csv_file(file_path, required_cols):
            return False
        
        try:
            df = pd.read_csv(file_path)
            
            # Data cleaning
            df = df.dropna(subset=['year', 'player_name', 'stat_category', 'stat_value'])
            df['team'] = df['team'].fillna('')
            
            # Data validation
            df = df[df['stat_value'].notna()]
            df = df[df['player_name'].str.len() > 0]
            
            # Convert data types
            df['year'] = df['year'].astype(int)
            df['stat_value'] = df['stat_value'].astype(float)
            
            # Insert data
            insert_sql = """
            INSERT INTO hitting_leaders (year, player_name, team, stat_category, stat_value)
            VALUES (?, ?, ?, ?, ?)
            """
            
            data_tuples = [
                (row['year'], row['player_name'], row['team'], row['stat_category'], row['stat_value'])
                for _, row in df.iterrows()
            ]
            
            self.conn.executemany(insert_sql, data_tuples)
            self.conn.commit()
            
            print(f"Imported {len(df)} hitting leader records")
            return True
            
        except Exception as e:
            print(f"Error importing hitting leaders: {e}")
            self.conn.rollback()
            return False
    
    def import_pitching_leaders(self, file_path: str = 'data/raw/yearly_pitching_leaders.csv'):
        """Import pitching leaders data"""
        required_cols = ['year', 'player_name', 'stat_category', 'stat_value']
        
        if not self.validate_csv_file(file_path, required_cols):
            return False
        
        try:
            df = pd.read_csv(file_path)
            
            # Data cleaning
            df = df.dropna(subset=['year', 'player_name', 'stat_category', 'stat_value'])
            df['team'] = df['team'].fillna('')
            
            # Data validation
            df = df[df['stat_value'].notna()]
            df = df[df['player_name'].str.len() > 0]
            
            # Convert data types
            df['year'] = df['year'].astype(int)
            df['stat_value'] = df['stat_value'].astype(float)
            
            # Insert data
            insert_sql = """
            INSERT INTO pitching_leaders (year, player_name, team, stat_category, stat_value)
            VALUES (?, ?, ?, ?, ?)
            """
            
            data_tuples = [
                (row['year'], row['player_name'], row['team'], row['stat_category'], row['stat_value'])
                for _, row in df.iterrows()
            ]
            
            self.conn.executemany(insert_sql, data_tuples)
            self.conn.commit()
            
            print(f"Imported {len(df)} pitching leader records")
            return True
            
        except Exception as e:
            print(f"Error importing pitching leaders: {e}")
            self.conn.rollback()
            return False
    
    def import_notable_events(self, file_path: str = 'data/raw/notable_events.csv'):
        """Import notable events data"""
        required_cols = ['year', 'description', 'event_type']
        
        if not self.validate_csv_file(file_path, required_cols):
            return False
        
        try:
            df = pd.read_csv(file_path)
            
            # Data cleaning
            df = df.dropna(subset=required_cols)
            df = df[df['description'].str.len() > 10]  # Minimum description length
            
            # Convert data types
            df['year'] = df['year'].astype(int)
            
            # Insert data
            insert_sql = """
            INSERT INTO notable_events (year, description, event_type)
            VALUES (?, ?, ?)
            """
            
            data_tuples = [
                (row['year'], row['description'], row['event_type'])
                for _, row in df.iterrows()
            ]
            
            self.conn.executemany(insert_sql, data_tuples)
            self.conn.commit()
            
            print(f"Imported {len(df)} notable event records")
            return True
            
        except Exception as e:
            print(f"Error importing notable events: {e}")
            self.conn.rollback()
            return False
    
    def create_indexes(self):
        """Create indexes for better query performance"""
        indexes = [
            "CREATE INDEX IF NOT EXISTS idx_standings_year ON standings(year);",
            "CREATE INDEX IF NOT EXISTS idx_standings_team ON standings(team_name);",
            "CREATE INDEX IF NOT EXISTS idx_hitting_year ON hitting_leaders(year);",
            "CREATE INDEX IF NOT EXISTS idx_hitting_player ON hitting_leaders(player_name);",
            "CREATE INDEX IF NOT EXISTS idx_hitting_category ON hitting_leaders(stat_category);",
            "CREATE INDEX IF NOT EXISTS idx_pitching_year ON pitching_leaders(year);",
            "CREATE INDEX IF NOT EXISTS idx_pitching_player ON pitching_leaders(player_name);",
            "CREATE INDEX IF NOT EXISTS idx_pitching_category ON pitching_leaders(stat_category);",
            "CREATE INDEX IF NOT EXISTS idx_events_year ON notable_events(year);",
            "CREATE INDEX IF NOT EXISTS idx_events_type ON notable_events(event_type);"
        ]
        
        for index_sql in indexes:
            try:
                self.conn.execute(index_sql)
            except sqlite3.Error as e:
                print(f"Warning: Could not create index: {e}")
        
        self.conn.commit()
        print("Created database indexes")
    
    def add_foreign_keys(self):
        """Add foreign key constraints after data is imported"""
        try:
            # Enable foreign keys
            self.conn.execute("PRAGMA foreign_keys = ON")
            
            # Create new tables with foreign keys and copy data
            alter_sql = [
                """
                CREATE TABLE hitting_leaders_new (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    year INTEGER NOT NULL,
                    player_name TEXT NOT NULL,
                    team TEXT,
                    stat_category TEXT NOT NULL,
                    stat_value REAL NOT NULL,
                    FOREIGN KEY (year) REFERENCES standings(year)
                );
                """,
                """
                INSERT INTO hitting_leaders_new 
                SELECT * FROM hitting_leaders;
                """,
                "DROP TABLE hitting_leaders;",
                "ALTER TABLE hitting_leaders_new RENAME TO hitting_leaders;",
                
                """
                CREATE TABLE pitching_leaders_new (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    year INTEGER NOT NULL,
                    player_name TEXT NOT NULL,
                    team TEXT,
                    stat_category TEXT NOT NULL,
                    stat_value REAL NOT NULL,
                    FOREIGN KEY (year) REFERENCES standings(year)
                );
                """,
                """
                INSERT INTO pitching_leaders_new 
                SELECT * FROM pitching_leaders;
                """,
                "DROP TABLE pitching_leaders;",
                "ALTER TABLE pitching_leaders_new RENAME TO pitching_leaders;",
                
                """
                CREATE TABLE notable_events_new (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    year INTEGER NOT NULL,
                    description TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    description_length INTEGER GENERATED ALWAYS AS (LENGTH(description)) STORED,
                    FOREIGN KEY (year) REFERENCES standings(year)
                );
                """,
                """
                INSERT INTO notable_events_new 
                SELECT id, year, description, event_type FROM notable_events;
                """,
                "DROP TABLE notable_events;",
                "ALTER TABLE notable_events_new RENAME TO notable_events;"
            ]
            
            for sql in alter_sql:
                self.conn.execute(sql)
            
            self.conn.commit()
            print("Added foreign key constraints")
            
        except sqlite3.Error as e:
            print(f"Warning: Could not add foreign keys: {e}")
    
    def verify_import(self):
        """Verify data was imported correctly"""
        print("\n=== DATABASE VERIFICATION ===")
        
        # Check record counts
        tables = ['standings', 'hitting_leaders', 'pitching_leaders', 'notable_events']
        
        for table in tables:
            try:
                cursor = self.conn.execute(f"SELECT COUNT(*) FROM {table}")
                count = cursor.fetchone()[0]
                print(f"{table}: {count} records")
            except sqlite3.Error as e:
                print(f"Error checking {table}: {e}")
        
        # Check year coverage
        try:
            cursor = self.conn.execute("SELECT DISTINCT year FROM standings ORDER BY year")
            years = [row[0] for row in cursor.fetchall()]
            print(f"Years covered: {years}")
        except sqlite3.Error as e:
            print(f"Error checking years: {e}")
        
        # Sample data check - join test
        try:
            cursor = self.conn.execute("""
                SELECT s.year, s.team_name, s.wins, s.losses, 
                       COUNT(DISTINCT h.id) as hitting_records,
                       COUNT(DISTINCT p.id) as pitching_records,
                       COUNT(DISTINCT e.id) as event_records
                FROM standings s
                LEFT JOIN hitting_leaders h ON s.year = h.year
                LEFT JOIN pitching_leaders p ON s.year = p.year
                LEFT JOIN notable_events e ON s.year = e.year
                WHERE s.year = 1927
                GROUP BY s.year, s.team_name, s.wins, s.losses
                ORDER BY s.wins DESC
                LIMIT 3
            """)
            
            results = cursor.fetchall()
            if results:
                print("\nSample join test (1927 season):")
                for row in results:
                    year, team, wins, losses, hitting, pitching, events = row
                    print(f"  {team}: {wins}-{losses} (H:{hitting}, P:{pitching}, E:{events})")
            
        except sqlite3.Error as e:
            print(f"Error in sample check: {e}")
    
    def run_import(self):
        """Run the complete import process"""
        print("Starting MLB database import...")
        
        try:
            # Connect to database
            self.connect()
            
            # Create tables (without foreign keys initially)
            self.create_tables()
            
            # Import data in order
            success = True
            success &= self.import_standings()
            success &= self.import_hitting_leaders()
            success &= self.import_pitching_leaders()  
            success &= self.import_notable_events()
            
            if success:
                # Create indexes
                self.create_indexes()
                
                # Add foreign keys after import
                self.add_foreign_keys()
                
                # Verify import
                self.verify_import()
                
                print("\nDatabase import completed successfully!")
                print(f"Database location: {os.path.abspath(self.db_path)}")
            else:
                print("\nDatabase import completed with errors")
                
        except Exception as e:
            print(f"\nImport failed: {e}")
        finally:
            if self.conn:
                self.conn.close()

if __name__ == "__main__":
    importer = MLBDatabaseImporter()
    importer.run_import()