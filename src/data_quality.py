import os
import re
import pandas as pd
import numpy as np
import logging
from typing import List, Dict, Optional, Tuple, Set
from dataclasses import dataclass
from enum import Enum


class QualityLevel(Enum):
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"
    INVALID = "invalid"


@dataclass
class QualityIssue:
    record_id: str
    field: str
    issue_type: str
    description: str
    severity: QualityLevel
    suggested_fix: Optional[str] = None


class BaseballDataValidator:
    def __init__(self):
        self.setup_logging()
        self.quality_issues: List[QualityIssue] = []
        
        # Valid hitting statistics
        self.valid_hitting_stats = {
            'Batting Average', 'Home Runs', 'RBIs', 'Hits', 'Runs',
            'Doubles', 'Triples', 'Total Bases', 'Slugging Average', 
            'On-Base Percentage', 'Walks'
        }
        
        # Valid pitching statistics  
        self.valid_pitching_stats = {
            'Wins', 'Losses', 'ERA', 'Strikeouts', 'WHIP', 
            'Saves', 'Games Started', 'Complete Games', 'Shutouts',
            'Innings Pitched', 'Games'
        }
        
        # Valid event types for notable events
        self.valid_event_types = {
            'Record', 'Retirement', 'Debut', 'Death', 'No-Hitter', 
            'World Series', 'Achievement', 'Milestone', 'Historical'
        }
        
        # Statistical ranges for validation
        self.stat_ranges = {
            "Home Runs": {"min": 1, "max": 75, "typical_max": 65},
            "RBIs": {"min": 10, "max": 200, "typical_max": 165},
            "Hits": {"min": 50, "max": 300, "typical_max": 250},
            "Runs": {"min": 20, "max": 200, "typical_max": 150},
            "Batting Average": {"min": 0.180, "max": 0.450, "typical_max": 0.400},
            "Doubles": {"min": 5, "max": 70, "typical_max": 60},
            "Triples": {"min": 1, "max": 30, "typical_max": 25},
            "Total Bases": {"min": 50, "max": 500, "typical_max": 450},
            "Slugging Average": {"min": 0.300, "max": 0.900, "typical_max": 0.800},
            "On-Base Percentage": {"min": 0.250, "max": 0.550, "typical_max": 0.500},
            "Walks": {"min": 20, "max": 200, "typical_max": 170},
            "ERA": {"min": 1.00, "max": 8.00, "typical_max": 6.00},
            "Wins": {"min": 5, "max": 35, "typical_max": 30},
            "Losses": {"min": 5, "max": 25, "typical_max": 20},
            "Strikeouts": {"min": 50, "max": 400, "typical_max": 350},
            "Complete Games": {"min": 1, "max": 40, "typical_max": 35},
            "Shutouts": {"min": 1, "max": 15, "typical_max": 12},
            "Saves": {"min": 10, "max": 65, "typical_max": 55},
            "Games": {"min": 10, "max": 90, "typical_max": 80},
            "WHIP": {"min": 0.80, "max": 2.50, "typical_max": 2.00}
        }
        
        # Team name patterns for identification
        self.team_keywords = {
            'yankees', 'red sox', 'tigers', 'white sox', 'athletics', 'orioles', 
            'angels', 'rangers', 'mariners', 'astros', 'twins', 'royals', 'indians', 
            'guardians', 'rays', 'brewers', 'cubs', 'pirates', 'cardinals', 'reds',
            'dodgers', 'giants', 'padres', 'rockies', 'diamondbacks', 'mets', 'phillies',
            'nationals', 'marlins', 'braves', 'senators', 'pilots', 'browns', 'cleveland',
            'new york', 'detroit', 'chicago', 'boston', 'minnesota', 'seattle', 
            'kansas city', 'tampa bay', 'texas', 'oakland', 'toronto', 'philadelphia'
        }
        
        # Words that indicate noise in participants field
        self.noise_words = {
            'On', 'The', 'Star', 'Game', 'Power', 'Rankings', 'Team', 
            'Standings', 'New', 'York', 'Series', 'Championship', 'During',
            'After', 'Before', 'When', 'Where', 'How', 'Why', 'What'
        }

    def setup_logging(self):
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)

    def safe_str(self, value) -> str:
        return str(value).strip() if pd.notna(value) else ""

    def safe_float(self, value) -> Optional[float]:
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    def is_team_record(self, player_name: str, team: str = "") -> bool:
        """Detect if this is team-level data rather than individual player data"""
        name_lower = player_name.lower() if player_name else ""
        team_lower = team.lower() if team else ""
        
        # Direct team name matches
        if any(keyword in name_lower for keyword in self.team_keywords):
            return True
            
        # Multi-word team patterns
        if len(player_name.split()) > 2 and any(word in name_lower for word in ['yankees', 'red sox', 'white sox']):
            return True
            
        # Statistical category names appearing as player names
        stat_indicators = [
            'batting average', 'home runs', 'doubles', 'triples', 'hits', 'runs',
            'total bases', 'slugging', 'wins', 'losses', 'era', 'strikeouts', 'saves'
        ]
        if any(indicator in name_lower for indicator in stat_indicators):
            return True
            
        return False

    def has_field_confusion(self, record: Dict) -> bool:
        """Check if player_name and stat_category fields are confused"""
        player = self.safe_str(record.get("player_name", ""))
        stat_cat = self.safe_str(record.get("stat_category", ""))
        
        # If player_name matches stat_category exactly
        if player == stat_cat:
            return True
            
        # If player_name contains statistical category names
        stat_categories = [
            'batting average', 'home runs', 'doubles', 'triples', 'hits', 'runs',
            'stolen bases', 'total bases', 'slugging average', 'wins', 'losses',
            'games', 'strikeouts', 'saves', 'era', 'complete games', 'shutouts'
        ]
        player_lower = player.lower()
        if any(stat_cat.lower() == player_lower for stat_cat in stat_categories):
            return True
            
        # Check if it's exactly a stat category name
        if player in ['Batting Average', 'Home Runs', 'Doubles', 'Triples', 'Hits', 'Runs',
                      'Stolen Bases', 'Total Bases', 'Slugging Average', 'Wins', 'Losses',
                      'Games', 'Strikeouts', 'Saves', 'ERA', 'Complete Games', 'Shutouts']:
            return True
            
        return False

    def fix_field_order(self, df: pd.DataFrame) -> pd.DataFrame:
        """Fix field order when player_name and team are swapped"""
        df_fixed = df.copy()
        
        # Find rows where player_name contains stat categories
        stat_categories = [
            'Batting Average', 'Home Runs', 'Doubles', 'Triples', 'Hits', 'Runs',
            'Stolen Bases', 'Total Bases', 'Slugging Average', 'Games', 'Wins', 'Losses'
        ]
        
        for idx, row in df_fixed.iterrows():
            player_name = self.safe_str(row.get('player_name', ''))
            team = self.safe_str(row.get('team', ''))
            
            # If player_name is actually a stat category, swap the fields
            if player_name in stat_categories and len(team) > 3:
                # Swap: real player name is in 'team' field
                df_fixed.loc[idx, 'player_name'] = team
                df_fixed.loc[idx, 'team'] = 'Unknown'  # We'll need to get team info elsewhere
                
                self.logger.info(f"Fixed field order: '{player_name}' -> player='{team}', stat='{player_name}'")
        
        return df_fixed

    def clean_hitting_leaders(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, List[QualityIssue]]:
        """Clean hitting leaders data with specific focus on identified issues"""
        self.logger.info("Cleaning hitting leaders data...")
        issues = []
        original_count = len(df)
        
        # First, fix field order issues
        df_fixed = self.fix_field_order(df)
        
        # Show examples of what was fixed
        self.logger.info("Examples of field order fixes applied")
        
        # Now check for remaining issues after fixing
        remaining_confusion = df_fixed[df_fixed.apply(lambda row: self.has_field_confusion(row.to_dict()), axis=1)]
        self.logger.info(f"Remaining field confusion issues: {len(remaining_confusion)}")
        
        # Filter out any remaining team records (after field fixes)
        team_mask = df_fixed.apply(lambda row: self.is_team_record(
            self.safe_str(row.get("player_name", "")),
            self.safe_str(row.get("team", ""))
        ), axis=1)
        
        df_clean = df_fixed[~team_mask].copy()
        team_records_removed = team_mask.sum()
        self.logger.info(f"Removed {team_records_removed} remaining team-level records")
        
        # Remove any remaining records with field confusion (should be very few now)
        confusion_mask = df_clean.apply(lambda row: self.has_field_confusion(row.to_dict()), axis=1)
        df_clean = df_clean[~confusion_mask]
        confusion_removed = confusion_mask.sum()
        self.logger.info(f"Removed {confusion_removed} remaining records with field confusion")
        
        # Filter to valid hitting statistics only
        valid_stats_mask = df_clean['stat_category'].isin(self.valid_hitting_stats)
        invalid_stats = df_clean[~valid_stats_mask]['stat_category'].unique()
        if len(invalid_stats) > 0:
            self.logger.info(f"Invalid stats found: {list(invalid_stats)}")
        
        df_clean = df_clean[valid_stats_mask]
        invalid_stats_removed = (~valid_stats_mask).sum()
        self.logger.info(f"Removed {invalid_stats_removed} records with invalid hitting stats")
        
        # Remove records with missing or invalid values
        before_cleanup = len(df_clean)
        df_clean = df_clean.dropna(subset=['player_name', 'stat_value'])
        df_clean = df_clean[df_clean['stat_value'] > 0]
        df_clean = df_clean[df_clean['player_name'].str.len() >= 3]
        after_cleanup = len(df_clean)
        self.logger.info(f"Removed {before_cleanup - after_cleanup} records with missing/invalid values")
        
        # Clean player names
        df_clean['player_name'] = df_clean['player_name'].str.replace(r'["\',]', '', regex=True)
        df_clean['player_name'] = df_clean['player_name'].str.strip()
        
        # Recalculate quality scores
        df_clean = self.recalculate_quality_scores(df_clean)
        
        final_count = len(df_clean)
        retention_rate = (final_count / original_count) * 100
        self.logger.info(f"Hitting leaders: {original_count} -> {final_count} ({retention_rate:.1f}% retained)")
        
        return df_clean.reset_index(drop=True), issues

    def clean_pitching_leaders(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, List[QualityIssue]]:
        """Clean pitching leaders data with field reordering fixes"""
        self.logger.info("Cleaning pitching leaders data...")
        issues = []
        original_count = len(df)
        
        # Fix field confusion - when player_name contains stat categories
        stat_category_names = ['Games', 'Wins', 'Losses', 'Strikeouts', 'Saves']
        
        for idx, row in df.iterrows():
            player_name = self.safe_str(row.get('player_name', ''))
            team = self.safe_str(row.get('team', ''))
            stat_category = self.safe_str(row.get('stat_category', ''))
            
            # If player_name is actually a stat category
            if player_name in stat_category_names:
                # Shift fields: player_name -> team, team -> unknown
                df.loc[idx, 'player_name'] = team
                df.loc[idx, 'team'] = 'Unknown'
                
                # Fix stat_category if it was "Unknown Pitching Stat"
                if stat_category == 'Unknown Pitching Stat':
                    df.loc[idx, 'stat_category'] = player_name
        
        # Remove records with "Unknown Pitching Stat" that couldn't be fixed
        unknown_mask = df['stat_category'] == 'Unknown Pitching Stat'
        df_clean = df[~unknown_mask].copy()
        unknown_removed = unknown_mask.sum()
        self.logger.info(f"Removed {unknown_removed} records with Unknown Pitching Stat")
        
        # Filter to valid pitching statistics only
        valid_stats_mask = df_clean['stat_category'].isin(self.valid_pitching_stats)
        df_clean = df_clean[valid_stats_mask]
        invalid_stats_removed = (~valid_stats_mask).sum()
        self.logger.info(f"Removed {invalid_stats_removed} records with invalid pitching stats")
        
        # Remove records with missing or invalid values
        df_clean = df_clean.dropna(subset=['player_name', 'stat_value'])
        df_clean = df_clean[df_clean['stat_value'] >= 0]
        df_clean = df_clean[df_clean['player_name'].str.len() >= 3]
        
        # Clean player names
        df_clean['player_name'] = df_clean['player_name'].str.replace(r'["\',]', '', regex=True)
        df_clean['player_name'] = df_clean['player_name'].str.strip()
        
        # Recalculate quality scores
        df_clean = self.recalculate_quality_scores(df_clean)
        
        final_count = len(df_clean)
        retention_rate = (final_count / original_count) * 100
        self.logger.info(f"Pitching leaders: {original_count} -> {final_count} ({retention_rate:.1f}% retained)")
        
        return df_clean.reset_index(drop=True), issues

    def clean_notable_events(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean notable events data focusing on baseball relevance and data quality"""
        self.logger.info("Cleaning notable events data...")
        original_count = len(df)
        
        # Remove system/aggregated records
        system_mask = (
            df['description'].str.contains('Team Standings|All-Star Game', na=False, case=False) |
            df['description'].str.match(r'^\d{4}.*\|.*', na=False)
        )
        df_clean = df[~system_mask].copy()
        system_removed = system_mask.sum()
        self.logger.info(f"Removed {system_removed} system/aggregated records")
        
        # Filter for baseball-related events
        baseball_keywords = [
            'baseball', 'Major League', 'Yankees', 'Red Sox', 'home run', 'pitcher',
            'World Series', 'batting', 'strikeout', 'rookie', 'manager', 'stadium',
            'game', 'season', 'record', 'debut', 'retirement', 'no-hitter', 'MVP',
            'All-Star', 'pennant', 'championship', 'playoff'
        ]
        
        baseball_mask = df_clean['description'].str.contains(
            '|'.join(baseball_keywords), case=False, na=False
        )
        
        # Keep historically significant non-baseball events as context
        historical_mask = df_clean['description'].str.contains(
            'Nixon|September 11|9/11|World Trade Center|President', case=False, na=False
        )
        
        keep_mask = baseball_mask | historical_mask
        df_clean = df_clean[keep_mask]
        non_baseball_removed = (~keep_mask).sum()
        self.logger.info(f"Removed {non_baseball_removed} non-baseball events")
        
        # Clean event types
        df_clean['event_type'] = df_clean.apply(self.normalize_event_type, axis=1)
        
        # Filter to valid event types
        valid_events_mask = df_clean['event_type'].isin(self.valid_event_types)
        df_clean = df_clean[valid_events_mask]
        
        # Clean participants field
        df_clean['participants'] = df_clean['participants'].apply(self.clean_participants)
        
        # Clean description field
        df_clean['description'] = df_clean['description'].str.replace(r'&[a-z]+;', ' ', regex=True)
        df_clean['description'] = df_clean['description'].str.replace(r'\s+', ' ', regex=True)
        df_clean['description'] = df_clean['description'].str.strip()
        
        # Recreate significance from description
        df_clean['significance'] = df_clean['description'].str[:100] + '...'
        
        # Remove duplicates and very short descriptions
        df_clean = df_clean.drop_duplicates(subset=['year', 'description'])
        df_clean = df_clean[df_clean['description'].str.len() > 30]
        
        final_count = len(df_clean)
        retention_rate = (final_count / original_count) * 100
        self.logger.info(f"Notable events: {original_count} -> {final_count} ({retention_rate:.1f}% retained)")
        
        return df_clean.reset_index(drop=True)

    def normalize_event_type(self, row) -> str:
        """Normalize event_type based on description content"""
        event_type = row['event_type']
        description = row['description'].lower()
        
        # Fix misclassified events
        if event_type == 'Death' and any(word in description for word in ['nixon', 'president', 'politics']):
            return 'Historical'
        elif event_type == 'Death' and 'lindbergh' in description:
            return 'Achievement'
        elif event_type == 'Record' and any(word in description for word in ['record', 'set', 'broke', 'new']):
            return 'Record'
        elif 'retirement' in description or 'retired' in description:
            return 'Retirement'
        elif 'debut' in description or 'first' in description:
            return 'Debut'
        elif 'no-hitter' in description or 'no hitter' in description:
            return 'No-Hitter'
        elif 'world series' in description:
            return 'World Series'
        else:
            return event_type if event_type in self.valid_event_types else 'Historical'

    def clean_participants(self, participants_str: str) -> str:
        """Clean the participants field by removing noise words and formatting"""
        if pd.isna(participants_str):
            return ""
        
        # Split by commas
        parts = str(participants_str).split(',')
        cleaned_parts = []
        
        for part in parts:
            part = part.strip()
            words = part.split()
            
            # Filter out noise words and very short words
            filtered_words = [
                word for word in words 
                if word not in self.noise_words and len(word) > 2
            ]
            
            if filtered_words:
                cleaned_part = ' '.join(filtered_words)
                if len(cleaned_part) > 3:
                    cleaned_parts.append(cleaned_part)
        
        # Return up to 5 participants
        return ', '.join(cleaned_parts[:5])

    def recalculate_quality_scores(self, df: pd.DataFrame) -> pd.DataFrame:
        """Recalculate quality scores based on data completeness and validity"""
        df = df.copy()
        df['quality_score'] = 100
        
        # Penalize missing team information
        no_team_mask = df['team'].isna() | (df['team'] == 'Unknown') | (df['team'] == '')
        df.loc[no_team_mask, 'quality_score'] -= 25
        
        # Penalize short player names
        short_name_mask = df['player_name'].str.len() < 5
        df.loc[short_name_mask, 'quality_score'] -= 15
        
        # Penalize statistical outliers
        for stat_category in df['stat_category'].unique():
            if stat_category in self.stat_ranges:
                stat_mask = df['stat_category'] == stat_category
                stat_values = df.loc[stat_mask, 'stat_value']
                
                if len(stat_values) > 1:
                    ranges = self.stat_ranges[stat_category]
                    
                    # Hard outliers (outside min/max)
                    hard_outlier_mask = stat_mask & (
                        (df['stat_value'] < ranges['min']) | 
                        (df['stat_value'] > ranges['max'])
                    )
                    df.loc[hard_outlier_mask, 'quality_score'] -= 50
                    
                    # Soft outliers (above typical max)
                    soft_outlier_mask = stat_mask & (df['stat_value'] > ranges['typical_max'])
                    df.loc[soft_outlier_mask, 'quality_score'] -= 20
        
        # Set quality levels
        df['quality_level'] = pd.cut(
            df['quality_score'],
            bins=[0, 50, 70, 85, 100],
            labels=['invalid', 'low', 'medium', 'high'],
            include_lowest=True
        )
        
        return df

    def generate_quality_report(self, original_data: Dict[str, pd.DataFrame], 
                              cleaned_data: Dict[str, pd.DataFrame]) -> Dict:
        """Generate comprehensive quality report"""
        report = {}
        
        for dataset_name in original_data:
            if dataset_name not in cleaned_data:
                continue
                
            original_df = original_data[dataset_name]
            cleaned_df = cleaned_data[dataset_name]
            
            report[dataset_name] = {
                'original_rows': len(original_df),
                'cleaned_rows': len(cleaned_df),
                'retention_rate': round((len(cleaned_df) / len(original_df)) * 100, 1),
                'records_removed': len(original_df) - len(cleaned_df)
            }
            
            if 'quality_level' in cleaned_df.columns:
                report[dataset_name]['quality_distribution'] = cleaned_df['quality_level'].value_counts().to_dict()
            
            if 'player_name' in cleaned_df.columns:
                report[dataset_name]['unique_players'] = cleaned_df['player_name'].nunique()
            
            if 'year' in cleaned_df.columns:
                years = sorted(cleaned_df['year'].unique())
                report[dataset_name]['year_range'] = f"{years[0]} - {years[-1]}" if years else "N/A"
            
            if 'stat_category' in cleaned_df.columns:
                report[dataset_name]['stat_categories'] = sorted(cleaned_df['stat_category'].unique().tolist())
        
        return report

    def save_issues(self, issues: List[QualityIssue], filename: str):
        """Save quality issues to CSV file"""
        if not issues:
            self.logger.info("No quality issues to save")
            return
        
        df = pd.DataFrame([issue.__dict__ for issue in issues])
        df['severity'] = df['severity'].apply(lambda x: x.value)
        df.to_csv(filename, index=False)
        self.logger.info(f"Saved {len(issues)} quality issues to {filename}")


def run_pipeline():
    """Main pipeline execution function"""
    print("Running comprehensive baseball data quality pipeline...")
    
    # Setup directories
    os.makedirs("data/processed", exist_ok=True)
    
    validator = BaseballDataValidator()
    
    # File paths - working directly with raw data
    input_files = {
        'hitting_leaders': 'data/raw/yearly_hitting_leaders.csv',
        'pitching_leaders': 'data/raw/yearly_pitching_leaders.csv', 
        'notable_events': 'data/raw/notable_events.csv'
    }
    
    original_data = {}
    cleaned_data = {}
    all_issues = []
    
    # Process each dataset
    for dataset_name, file_path in input_files.items():
        try:
            print(f"Processing {dataset_name}...")
            df = pd.read_csv(file_path)
            original_data[dataset_name] = df.copy()
            
            if dataset_name == 'hitting_leaders':
                cleaned_df, issues = validator.clean_hitting_leaders(df)
                all_issues.extend(issues)
            elif dataset_name == 'pitching_leaders':
                cleaned_df, issues = validator.clean_pitching_leaders(df)
                all_issues.extend(issues)
            elif dataset_name == 'notable_events':
                cleaned_df = validator.clean_notable_events(df)
            else:
                cleaned_df = df
            
            cleaned_data[dataset_name] = cleaned_df
            
            # Save cleaned data directly as final files
            output_path = f"data/processed/{dataset_name}_final.csv"
            cleaned_df.to_csv(output_path, index=False)
            print(f"Saved cleaned data to {output_path}")
            
        except FileNotFoundError:
            print(f"Warning: {file_path} not found, skipping...")
            continue
        except Exception as e:
            print(f"Error processing {dataset_name}: {e}")
            continue
    
    # Generate and save quality report
    if original_data and cleaned_data:
        report = validator.generate_quality_report(original_data, cleaned_data)
        
        print("\nQuality Report Summary:")
        print("=" * 50)
        for dataset, stats in report.items():
            print(f"{dataset.upper()}:")
            print(f"  Rows: {stats['original_rows']} -> {stats['cleaned_rows']} ({stats['retention_rate']}% retained)")
            if 'unique_players' in stats:
                print(f"  Unique players: {stats['unique_players']}")
            if 'year_range' in stats:
                print(f"  Year range: {stats['year_range']}")
            if 'quality_distribution' in stats:
                print(f"  Quality: {stats['quality_distribution']}")
            print()
    
    # Save quality issues
    validator.save_issues(all_issues, "data/processed/quality_issues_final.csv")
    
    print(f"Pipeline completed. Total issues found: {len(all_issues)}")
    print("All final datasets saved with '_final.csv' suffix")


if __name__ == "__main__":
    run_pipeline()