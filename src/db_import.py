import sqlite3
import pandas as pd
import logging
import os
from typing import List, Dict, Optional
from datetime import datetime

class BaseballDatabaseImporter:
    """
    Import cleaned baseball data into SQLite database with proper schema design
    """
    
    def __init__(self, db_path: str = "database/baseball.db"):
        """Initialize database importer"""
        self.db_path = db_path
        self.setup_logging()
        
        # Ensure database directory exists
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        
        # Connect to database
        self.conn = sqlite3.connect(db_path)
        self.conn.execute("PRAGMA foreign_keys = ON")  # Enable foreign key constraints
        
    def setup_logging(self):
        """Setup logging for database operations"""
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
    
    def create_schema(self):
        """Create database schema with proper tables and relationships"""
        
        self.logger.info("Creating database schema...")
        
        # Drop existing tables if they exist (for clean start)
        drop_tables = [
            "DROP TABLE IF EXISTS hitting_leaders",
            "DROP TABLE IF EXISTS pitching_leaders", 
            "DROP TABLE IF EXISTS team_standings",
            "DROP TABLE IF EXISTS data_quality_log"
        ]
        
        for drop_sql in drop_tables:
            self.conn.execute(drop_sql)
        
        # Create hitting_leaders table
        hitting_table = """
        CREATE TABLE hitting_leaders (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            year INTEGER NOT NULL,
            rank INTEGER,
            player_name TEXT NOT NULL,
            team TEXT NOT NULL,
            stat_category TEXT NOT NULL,
            stat_value REAL NOT NULL,
            quality_score REAL DEFAULT 100.0,
            quality_level TEXT DEFAULT 'high',
            team_standardized BOOLEAN DEFAULT FALSE,
            stat_category_corrected BOOLEAN DEFAULT FALSE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(year, player_name, team, stat_category)
        )
        """
        
        # Create pitching_leaders table
        pitching_table = """
        CREATE TABLE pitching_leaders (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            year INTEGER NOT NULL,
            rank INTEGER,
            player_name TEXT NOT NULL,
            team TEXT NOT NULL,
            stat_category TEXT NOT NULL,
            stat_value REAL NOT NULL,
            quality_score REAL DEFAULT 100.0,
            quality_level TEXT DEFAULT 'high',
            team_standardized BOOLEAN DEFAULT FALSE,
            stat_category_corrected BOOLEAN DEFAULT FALSE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(year, player_name, team, stat_category)
        )
        """
        
        # Create team_standings table
        standings_table = """
        CREATE TABLE team_standings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            year INTEGER NOT NULL,
            team_name TEXT NOT NULL,
            wins INTEGER NOT NULL,
            losses INTEGER NOT NULL,
            win_pct REAL,
            games_behind INTEGER DEFAULT NULL,
            division TEXT DEFAULT NULL,
            league TEXT DEFAULT 'AL',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(year, team_name)
        )
        """
        
        # Create data quality audit log
        quality_log_table = """
        CREATE TABLE data_quality_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            record_id TEXT NOT NULL,
            table_name TEXT NOT NULL,
            field_name TEXT NOT NULL,
            issue_type TEXT NOT NULL,
            description TEXT NOT NULL,
            severity TEXT NOT NULL,
            suggested_fix TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        
        # Execute table creation
        tables = [hitting_table, pitching_table, standings_table, quality_log_table]
        for table_sql in tables:
            self.conn.execute(table_sql)
        
        # Create indexes for better query performance
        indexes = [
            "CREATE INDEX idx_hitting_year ON hitting_leaders(year)",
            "CREATE INDEX idx_hitting_player ON hitting_leaders(player_name)",
            "CREATE INDEX idx_hitting_team ON hitting_leaders(team)",
            "CREATE INDEX idx_hitting_stat ON hitting_leaders(stat_category)",
            "CREATE INDEX idx_hitting_quality ON hitting_leaders(quality_score)",
            
            "CREATE INDEX idx_pitching_year ON pitching_leaders(year)",
            "CREATE INDEX idx_pitching_player ON pitching_leaders(player_name)",
            "CREATE INDEX idx_pitching_team ON pitching_leaders(team)",
            "CREATE INDEX idx_pitching_stat ON pitching_leaders(stat_category)",
            
            "CREATE INDEX idx_standings_year ON team_standings(year)",
            "CREATE INDEX idx_standings_team ON team_standings(team_name)",
            
            "CREATE INDEX idx_quality_table ON data_quality_log(table_name)",
            "CREATE INDEX idx_quality_severity ON data_quality_log(severity)"
        ]
        
        for index_sql in indexes:
            self.conn.execute(index_sql)
        
        self.conn.commit()
        self.logger.info("Database schema created successfully")
    
    def import_hitting_data(self, csv_path: str = "data/processed/yearly_hitting_leaders_cleaned.csv"):
        """Import hitting leaders data"""
        
        try:
            self.logger.info(f"Importing hitting data from {csv_path}")
            
            df = pd.read_csv(csv_path)
            
            # Clean and prepare data
            df['rank'] = df['rank'].fillna(0)
            df['team_standardized'] = df['team_standardized'].fillna(False)
            df['stat_category_corrected'] = df['stat_category_corrected'].fillna(False)
            df['quality_score'] = df['quality_score'].fillna(100.0)
            df['quality_level'] = df['quality_level'].fillna('high')
            
            # Insert data
            records_inserted = 0
            for _, row in df.iterrows():
                try:
                    insert_sql = """
                    INSERT OR REPLACE INTO hitting_leaders 
                    (year, rank, player_name, team, stat_category, stat_value, 
                     quality_score, quality_level, team_standardized, stat_category_corrected)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """
                    
                    self.conn.execute(insert_sql, (
                        row['year'],
                        row['rank'] if pd.notna(row['rank']) else None,
                        row['player_name'],
                        row['team'],
                        row['stat_category'],
                        row['stat_value'],
                        row['quality_score'],
                        row['quality_level'],
                        bool(row['team_standardized']),
                        bool(row['stat_category_corrected'])
                    ))
                    records_inserted += 1
                    
                except sqlite3.Error as e:
                    self.logger.warning(f"Failed to insert hitting record: {row['player_name']} - {e}")
            
            self.conn.commit()
            self.logger.info(f"Imported {records_inserted} hitting records")
            return records_inserted
            
        except Exception as e:
            self.logger.error(f"Error importing hitting data: {e}")
            return 0
    
    def import_pitching_data(self, csv_path: str = "data/processed/yearly_pitching_leaders_cleaned.csv"):
        """Import pitching leaders data"""
        
        try:
            self.logger.info(f"Importing pitching data from {csv_path}")
            
            df = pd.read_csv(csv_path)
            
            # Clean and prepare data
            df['rank'] = df['rank'].fillna(0)
            df['team_standardized'] = df['team_standardized'].fillna(False)
            df['stat_category_corrected'] = df['stat_category_corrected'].fillna(False)
            df['quality_score'] = df['quality_score'].fillna(100.0)
            df['quality_level'] = df['quality_level'].fillna('high')
            
            # Insert data
            records_inserted = 0
            for _, row in df.iterrows():
                try:
                    insert_sql = """
                    INSERT OR REPLACE INTO pitching_leaders 
                    (year, rank, player_name, team, stat_category, stat_value,
                     quality_score, quality_level, team_standardized, stat_category_corrected)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """
                    
                    self.conn.execute(insert_sql, (
                        row['year'],
                        row['rank'] if pd.notna(row['rank']) else None,
                        row['player_name'],
                        row['team'],
                        row['stat_category'],
                        row['stat_value'],
                        row['quality_score'],
                        row['quality_level'],
                        bool(row['team_standardized']),
                        bool(row['stat_category_corrected'])
                    ))
                    records_inserted += 1
                    
                except sqlite3.Error as e:
                    self.logger.warning(f"Failed to insert pitching record: {row['player_name']} - {e}")
            
            self.conn.commit()
            self.logger.info(f"Imported {records_inserted} pitching records")
            return records_inserted
            
        except Exception as e:
            self.logger.error(f"Error importing pitching data: {e}")
            return 0
    
    def import_standings_data(self, csv_path: str = "data/raw/team_standings.csv"):
        """Import team standings data"""
        
        try:
            self.logger.info(f"Importing standings data from {csv_path}")
            
            df = pd.read_csv(csv_path)
            
            # Insert data
            records_inserted = 0
            for _, row in df.iterrows():
                try:
                    insert_sql = """
                    INSERT OR REPLACE INTO team_standings 
                    (year, team_name, wins, losses, win_pct)
                    VALUES (?, ?, ?, ?, ?)
                    """
                    
                    self.conn.execute(insert_sql, (
                        row['year'],
                        row['team_name'],
                        row['wins'],
                        row['losses'],
                        row['win_pct']
                    ))
                    records_inserted += 1
                    
                except sqlite3.Error as e:
                    self.logger.warning(f"Failed to insert standings record: {row['team_name']} - {e}")
            
            self.conn.commit()
            self.logger.info(f"Imported {records_inserted} team standings records")
            return records_inserted
            
        except Exception as e:
            self.logger.error(f"Error importing standings data: {e}")
            return 0
    
    def import_quality_log(self, csv_path: str = "data/processed/quality_report.csv"):
        """Import data quality audit log"""
        
        try:
            if not os.path.exists(csv_path):
                self.logger.info("No quality report found, skipping quality log import")
                return 0
                
            self.logger.info(f"Importing quality log from {csv_path}")
            
            df = pd.read_csv(csv_path)
            
            # Insert data
            records_inserted = 0
            for _, row in df.iterrows():
                try:
                    # Determine table name from record_id
                    table_name = "hitting_leaders"  # Default
                    if "pitching" in str(row.get('description', '')).lower():
                        table_name = "pitching_leaders"
                    
                    insert_sql = """
                    INSERT INTO data_quality_log 
                    (record_id, table_name, field_name, issue_type, description, severity, suggested_fix)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """
                    
                    self.conn.execute(insert_sql, (
                        row['record_id'],
                        table_name,
                        row['field'],
                        row['issue_type'],
                        row['description'],
                        row['severity'],
                        row.get('suggested_fix', None)
                    ))
                    records_inserted += 1
                    
                except sqlite3.Error as e:
                    self.logger.warning(f"Failed to insert quality log record: {e}")
            
            self.conn.commit()
            self.logger.info(f"Imported {records_inserted} quality log records")
            return records_inserted
            
        except Exception as e:
            self.logger.error(f"Error importing quality log: {e}")
            return 0
    
    def test_database_queries(self):
        """Test database with sample queries including joins"""
        
        self.logger.info("Testing database with sample queries...")
        
        queries = [
            {
                "name": "Count records by table",
                "sql": """
                SELECT 'hitting_leaders' as table_name, COUNT(*) as record_count
                FROM hitting_leaders
                UNION ALL
                SELECT 'pitching_leaders' as table_name, COUNT(*) as record_count  
                FROM pitching_leaders
                UNION ALL
                SELECT 'team_standings' as table_name, COUNT(*) as record_count
                FROM team_standings
                """
            },
            {
                "name": "Top home run hitters by year",
                "sql": """
                SELECT year, player_name, team, stat_value as home_runs
                FROM hitting_leaders 
                WHERE stat_category = 'Home Runs'
                ORDER BY year DESC, stat_value DESC
                LIMIT 10
                """
            },
            {
                "name": "Yankees players with team standings (JOIN example)",
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
                    AND (h.team LIKE '%Yankees%' OR t.team_name LIKE '%Yankees%')
                WHERE h.team LIKE '%Yankees%'
                ORDER BY h.year DESC, h.stat_value DESC
                LIMIT 10
                """
            },
            {
                "name": "Data quality summary",
                "sql": """
                SELECT 
                    quality_level,
                    COUNT(*) as record_count,
                    AVG(quality_score) as avg_quality_score
                FROM (
                    SELECT quality_level, quality_score FROM hitting_leaders
                    UNION ALL
                    SELECT quality_level, quality_score FROM pitching_leaders
                ) combined
                GROUP BY quality_level
                ORDER BY avg_quality_score DESC
                """
            }
        ]
        
        for query in queries:
            try:
                print(f"\n{query['name']}:")
                print("=" * 50)
                
                cursor = self.conn.execute(query['sql'])
                results = cursor.fetchall()
                columns = [description[0] for description in cursor.description]
                
                # Print column headers
                print(f"{' | '.join(columns)}")
                print("-" * 50)
                
                # Print results (limit to first 5 rows for readability)
                for row in results[:5]:
                    formatted_row = []
                    for item in row:
                        if isinstance(item, float):
                            formatted_row.append(f"{item:.3f}")
                        else:
                            formatted_row.append(str(item))
                    print(f"{' | '.join(formatted_row)}")
                
                if len(results) > 5:
                    print(f"... and {len(results) - 5} more rows")
                    
            except sqlite3.Error as e:
                self.logger.error(f"Query failed: {query['name']} - {e}")
    
    def get_database_stats(self) -> Dict:
        """Get comprehensive database statistics"""
        
        stats = {}
        
        try:
            # Table record counts
            tables = ['hitting_leaders', 'pitching_leaders', 'team_standings', 'data_quality_log']
            for table in tables:
                cursor = self.conn.execute(f"SELECT COUNT(*) FROM {table}")
                stats[f"{table}_count"] = cursor.fetchone()[0]
            
            # Year range
            cursor = self.conn.execute("SELECT MIN(year), MAX(year) FROM hitting_leaders")
            min_year, max_year = cursor.fetchone()
            stats['year_range'] = f"{min_year}-{max_year}"
            
            # Quality distribution
            cursor = self.conn.execute("""
                SELECT quality_level, COUNT(*) 
                FROM hitting_leaders 
                GROUP BY quality_level
            """)
            quality_dist = dict(cursor.fetchall())
            stats['quality_distribution'] = quality_dist
            
            return stats
            
        except sqlite3.Error as e:
            self.logger.error(f"Error getting database stats: {e}")
            return {}
    
    def close(self):
        """Close database connection"""
        if self.conn:
            self.conn.close()
            self.logger.info("Database connection closed")

def run_database_import():
    """Main function to run complete database import process"""
    
    print("Baseball Database Import - Step 4")
    print("=" * 50)
    
    importer = BaseballDatabaseImporter()
    
    try:
        # Create schema
        importer.create_schema()
        
        # Import all data
        hitting_count = importer.import_hitting_data()
        pitching_count = importer.import_pitching_data()
        standings_count = importer.import_standings_data()
        quality_count = importer.import_quality_log()
        
        # Get database statistics
        stats = importer.get_database_stats()
        
        # Print summary
        print(f"\nDATABASE IMPORT SUMMARY:")
        print(f"Hitting leaders imported: {hitting_count}")
        print(f"Pitching leaders imported: {pitching_count}")
        print(f"Team standings imported: {standings_count}")
        print(f"Quality log entries: {quality_count}")
        print(f"Year range: {stats.get('year_range', 'Unknown')}")
        
        if stats.get('quality_distribution'):
            print(f"\nQuality distribution:")
            for level, count in stats['quality_distribution'].items():
                print(f"  {level}: {count} records")
        
        # Test database with queries
        print(f"\nTesting database with sample queries:")
        importer.test_database_queries()
        
        print(f"\nDatabase created successfully: {importer.db_path}")
        print(f"Ready for Step 5: Database Query Tool!")
        
        return True
        
    except Exception as e:
        print(f"Database import failed: {e}")
        import traceback
        traceback.print_exc()
        return False
        
    finally:
        importer.close()

if __name__ == "__main__":
    success = run_database_import()
    if success:
        print("\nStep 4 completed successfully!")
    else:
        print("\nStep 4 failed - check error messages above")