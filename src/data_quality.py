import pandas as pd
import numpy as np
import logging
from typing import Dict, List, Tuple, Optional
import re
from dataclasses import dataclass
from enum import Enum
import os

class QualityLevel(Enum):
    HIGH = "high"
    MEDIUM = "medium"  
    LOW = "low"
    INVALID = "invalid"

@dataclass
class QualityIssue:
    """Represents a data quality issue"""
    record_id: str
    field: str
    issue_type: str
    description: str
    severity: QualityLevel
    suggested_fix: Optional[str] = None

class BaseballDataValidator:
    """
    Data quality pipeline for baseball statistics
    Validates, cleans, and enriches scraped data
    """
    
    def __init__(self):
        self.setup_logging()
        
        # Historical ranges for validation (based on MLB history)
        self.stat_ranges = {
            "Home Runs": {"min": 1, "max": 75, "typical_max": 65},
            "RBI": {"min": 10, "max": 200, "typical_max": 165},
            "Hits": {"min": 50, "max": 300, "typical_max": 250},
            "Runs": {"min": 20, "max": 200, "typical_max": 150},
            "Batting Average": {"min": 0.180, "max": 0.450, "typical_max": 0.400},
            "Doubles": {"min": 5, "max": 70, "typical_max": 60},
            "Triples": {"min": 1, "max": 30, "typical_max": 25},
            "Stolen Bases": {"min": 1, "max": 150, "typical_max": 100},
            "Total Bases": {"min": 50, "max": 500, "typical_max": 450},
            
            # Pitching stats
            "ERA": {"min": 1.00, "max": 8.00, "typical_max": 6.00},
            "Wins": {"min": 5, "max": 35, "typical_max": 30},
            "Strikeouts": {"min": 50, "max": 400, "typical_max": 350},
            "Complete Games": {"min": 1, "max": 40, "typical_max": 35},
            "Shutouts": {"min": 1, "max": 15, "typical_max": 12},
            "Saves": {"min": 10, "max": 65, "typical_max": 55},
            "Innings Pitched": {"min": 100, "max": 400, "typical_max": 350}
        }
        
        # Common team name mappings (for fixing team names)
        self.team_mappings = {
            "New York": "New York Yankees",
            "Yankees": "New York Yankees", 
            "Boston": "Boston Red Sox",
            "Detroit": "Detroit Tigers",
            "Chicago": "Chicago White Sox",
            "Philadelphia": "Philadelphia Athletics",
            "St. Louis": "St. Louis Browns",
            "Cleveland": "Cleveland Indians",
            "Washington": "Washington Senators"
        }
        
        self.quality_issues = []
        
    def setup_logging(self):
        """Setup logging for data quality pipeline"""
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
    
    def safe_str(self, value) -> str:
        """Safely convert any value to string"""
        if pd.isna(value) or value is None:
            return ""
        return str(value).strip()
    
    def safe_float(self, value) -> Optional[float]:
        """Safely convert value to float"""
        if pd.isna(value) or value is None:
            return None
        try:
            return float(value)
        except (ValueError, TypeError):
            return None
    
    def validate_hitting_record(self, record: Dict) -> Tuple[Dict, List[QualityIssue]]:
        """
        Validate and clean a single hitting record
        """
        issues = []
        cleaned_record = record.copy()
        
        # Safe extraction of fields
        player_name = self.safe_str(record.get('player_name', ''))
        team = self.safe_str(record.get('team', ''))
        stat_category = self.safe_str(record.get('stat_category', ''))
        stat_value = self.safe_float(record.get('stat_value'))
        year = record.get('year', 'unknown')
        
        record_id = f"{year}-{player_name}"
        
        # 1. Validate player name
        if not player_name or len(player_name) < 3:
            issues.append(QualityIssue(
                record_id=record_id,
                field="player_name",
                issue_type="missing_value",
                description="Player name is missing or too short",
                severity=QualityLevel.INVALID
            ))
        elif not re.match(r'^[A-Z][a-z]+ [A-Z][a-z]+', player_name):
            issues.append(QualityIssue(
                record_id=record_id,
                field="player_name", 
                issue_type="format_issue",
                description=f"Player name format unusual: {player_name}",
                severity=QualityLevel.MEDIUM
            ))
        
        # 2. Validate and fix team name
        if team in self.team_mappings:
            cleaned_record['team'] = self.team_mappings[team]
            cleaned_record['team_standardized'] = True
        elif len(team) < 3:
            issues.append(QualityIssue(
                record_id=record_id,
                field="team",
                issue_type="missing_value", 
                description="Team name is missing or too short",
                severity=QualityLevel.HIGH
            ))
        
        # 3. Validate statistical category and value
        if stat_category in self.stat_ranges and stat_value is not None:
            ranges = self.stat_ranges[stat_category]
            
            # Check if value is completely out of range
            if stat_value < ranges["min"] or stat_value > ranges["max"]:
                # Try to fix common misclassifications
                fixed_category = self.suggest_stat_category_fix(stat_value, stat_category, "hitting")
                if fixed_category:
                    issues.append(QualityIssue(
                        record_id=record_id,
                        field="stat_category",
                        issue_type="misclassification",
                        description=f"Value {stat_value} unusual for {stat_category}, changed to {fixed_category}",
                        severity=QualityLevel.MEDIUM,
                        suggested_fix=f"Changed from {stat_category} to {fixed_category}"
                    ))
                    cleaned_record['stat_category'] = fixed_category
                    cleaned_record['stat_category_corrected'] = True
                else:
                    issues.append(QualityIssue(
                        record_id=record_id,
                        field="stat_value",
                        issue_type="out_of_range",
                        description=f"Value {stat_value} outside expected range for {stat_category}",
                        severity=QualityLevel.HIGH
                    ))
            
            # Check if value is suspicious (within range but unusual)
            elif stat_value > ranges["typical_max"]:
                issues.append(QualityIssue(
                    record_id=record_id,
                    field="stat_value", 
                    issue_type="suspicious_value",
                    description=f"Value {stat_value} is unusually high for {stat_category}",
                    severity=QualityLevel.MEDIUM
                ))
        elif stat_value is None and stat_category:
            issues.append(QualityIssue(
                record_id=record_id,
                field="stat_value",
                issue_type="missing_value",
                description="Statistical value is missing",
                severity=QualityLevel.HIGH
            ))
        
        # 4. Add quality score
        quality_score = self.calculate_quality_score(issues)
        cleaned_record['quality_score'] = quality_score
        cleaned_record['quality_level'] = self.get_quality_level(quality_score).value
        
        return cleaned_record, issues
    
    def validate_pitching_record(self, record: Dict) -> Tuple[Dict, List[QualityIssue]]:
        """Validate and clean a single pitching record"""
        issues = []
        cleaned_record = record.copy()
        
        # Safe extraction of fields
        player_name = self.safe_str(record.get('player_name', ''))
        team = self.safe_str(record.get('team', ''))
        stat_category = self.safe_str(record.get('stat_category', ''))
        stat_value = self.safe_float(record.get('stat_value'))
        year = record.get('year', 'unknown')
        
        record_id = f"{year}-{player_name}"
        
        # Similar validation as hitting, but for pitching stats
        if stat_category in self.stat_ranges and stat_value is not None:
            ranges = self.stat_ranges[stat_category]
            
            if stat_value < ranges["min"] or stat_value > ranges["max"]:
                fixed_category = self.suggest_stat_category_fix(stat_value, stat_category, "pitching")
                if fixed_category:
                    cleaned_record['stat_category'] = fixed_category
                    cleaned_record['stat_category_corrected'] = True
                    issues.append(QualityIssue(
                        record_id=record_id,
                        field="stat_category",
                        issue_type="misclassification",
                        description=f"Corrected {stat_category} to {fixed_category}",
                        severity=QualityLevel.MEDIUM
                    ))
                else:
                    issues.append(QualityIssue(
                        record_id=record_id,
                        field="stat_value",
                        issue_type="out_of_range", 
                        description=f"Value {stat_value} outside expected range for {stat_category}",
                        severity=QualityLevel.HIGH
                    ))
        
        # Validate team name
        if team in self.team_mappings:
            cleaned_record['team'] = self.team_mappings[team]
            cleaned_record['team_standardized'] = True
        
        # Add quality metrics
        quality_score = self.calculate_quality_score(issues)
        cleaned_record['quality_score'] = quality_score
        cleaned_record['quality_level'] = self.get_quality_level(quality_score).value
        
        return cleaned_record, issues
    
    def suggest_stat_category_fix(self, value: float, current_category: str, stat_type: str) -> Optional[str]:
        """
        Suggest a better statistical category based on value ranges
        """
        if stat_type == "hitting":
            # Common misclassifications in hitting
            if 100 <= value <= 300:  # Likely hits, not home runs
                if current_category == "Home Runs":
                    return "Hits"
            elif 50 <= value <= 200 and current_category == "Home Runs":  # Likely RBI
                return "RBI"
            elif 200 <= value <= 500 and current_category in ["Home Runs", "RBI"]:  # Likely total bases
                return "Total Bases"
            elif 20 <= value <= 80 and current_category == "Hits":  # Likely home runs
                return "Home Runs"
        
        elif stat_type == "pitching":
            # Common pitching misclassifications  
            if 15 <= value <= 40 and current_category == "ERA":  # Likely complete games or wins
                return "Complete Games"
            elif 100 <= value <= 400 and current_category in ["Wins", "ERA"]:  # Likely strikeouts
                return "Strikeouts"
        
        return None
    
    def calculate_quality_score(self, issues: List[QualityIssue]) -> float:
        """Calculate a quality score from 0-100 based on issues"""
        if not issues:
            return 100.0
        
        penalty = 0
        for issue in issues:
            if issue.severity == QualityLevel.INVALID:
                penalty += 50
            elif issue.severity == QualityLevel.HIGH:
                penalty += 25
            elif issue.severity == QualityLevel.MEDIUM:
                penalty += 10
            else:  # LOW
                penalty += 5
        
        return max(0, 100 - penalty)
    
    def get_quality_level(self, score: float) -> QualityLevel:
        """Convert numeric score to quality level"""
        if score >= 90:
            return QualityLevel.HIGH
        elif score >= 70:
            return QualityLevel.MEDIUM
        elif score >= 50:
            return QualityLevel.LOW
        else:
            return QualityLevel.INVALID
    
    def process_hitting_data(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, List[QualityIssue]]:
        """Process entire hitting dataset"""
        self.logger.info(f"Processing {len(df)} hitting records")
        
        cleaned_records = []
        all_issues = []
        
        for _, record in df.iterrows():
            cleaned_record, issues = self.validate_hitting_record(record.to_dict())
            cleaned_records.append(cleaned_record)
            all_issues.extend(issues)
        
        cleaned_df = pd.DataFrame(cleaned_records)
        
        self.logger.info(f"Found {len(all_issues)} quality issues in hitting data")
        return cleaned_df, all_issues
    
    def process_pitching_data(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, List[QualityIssue]]:
        """Process entire pitching dataset"""
        self.logger.info(f"Processing {len(df)} pitching records")
        
        cleaned_records = []
        all_issues = []
        
        for _, record in df.iterrows():
            cleaned_record, issues = self.validate_pitching_record(record.to_dict())
            cleaned_records.append(cleaned_record)
            all_issues.extend(issues)
        
        cleaned_df = pd.DataFrame(cleaned_records)
        
        self.logger.info(f"Found {len(all_issues)} quality issues in pitching data")
        return cleaned_df, all_issues
    
    def generate_quality_report(self, issues: List[QualityIssue]) -> Dict:
        """Generate comprehensive quality report"""
        if not issues:
            return {"total_issues": 0, "quality_summary": "No issues found"}
        
        # Group issues by type and severity
        by_severity = {}
        by_type = {}
        by_field = {}
        
        for issue in issues:
            # By severity
            severity_key = issue.severity.value
            by_severity[severity_key] = by_severity.get(severity_key, 0) + 1
            
            # By type
            by_type[issue.issue_type] = by_type.get(issue.issue_type, 0) + 1
            
            # By field
            by_field[issue.field] = by_field.get(issue.field, 0) + 1
        
        # Calculate overall quality metrics
        total_issues = len(issues)
        critical_issues = by_severity.get('invalid', 0) + by_severity.get('high', 0)
        
        return {
            "total_issues": total_issues,
            "critical_issues": critical_issues,
            "by_severity": by_severity,
            "by_type": by_type,
            "by_field": by_field,
            "quality_summary": f"{total_issues} total issues, {critical_issues} critical"
        }
    
    def save_quality_report(self, issues: List[QualityIssue], filename: str = "data/processed/quality_report.csv"):
        """Save quality issues to CSV for analysis"""
        os.makedirs("data/processed", exist_ok=True)
        
        if issues:
            issues_data = []
            for issue in issues:
                issues_data.append({
                    "record_id": issue.record_id,
                    "field": issue.field,
                    "issue_type": issue.issue_type,
                    "description": issue.description,
                    "severity": issue.severity.value,
                    "suggested_fix": issue.suggested_fix
                })
            
            issues_df = pd.DataFrame(issues_data)
            issues_df.to_csv(filename, index=False)
            self.logger.info(f"Saved {len(issues)} quality issues to {filename}")

def run_data_quality_pipeline():
    """
    Main function to run the complete data quality pipeline
    """
    print("Baseball Data Quality Pipeline")
    print("="*50)
    
    validator = BaseballDataValidator()
    
    try:
        # Load raw data
        hitting_df = pd.read_csv("data/raw/yearly_hitting_leaders.csv")
        pitching_df = pd.read_csv("data/raw/yearly_pitching_leaders.csv")
        
        print(f"Loaded {len(hitting_df)} hitting records and {len(pitching_df)} pitching records")
        
        # Process hitting data
        print("\nProcessing hitting data...")
        cleaned_hitting, hitting_issues = validator.process_hitting_data(hitting_df)
        
        # Process pitching data  
        print("Processing pitching data...")
        cleaned_pitching, pitching_issues = validator.process_pitching_data(pitching_df)
        
        # Combine all issues
        all_issues = hitting_issues + pitching_issues
        
        # Generate quality report
        quality_report = validator.generate_quality_report(all_issues)
        
        # Save cleaned data
        os.makedirs("data/processed", exist_ok=True)
        
        cleaned_hitting.to_csv("data/processed/yearly_hitting_leaders_cleaned.csv", index=False)
        cleaned_pitching.to_csv("data/processed/yearly_pitching_leaders_cleaned.csv", index=False)
        
        # Save quality report
        validator.save_quality_report(all_issues)
        
        # Print summary
        print(f"\nDATA QUALITY SUMMARY:")
        print(f"Total records processed: {len(hitting_df) + len(pitching_df)}")
        print(f"Quality issues found: {quality_report['total_issues']}")
        print(f"Critical issues: {quality_report['critical_issues']}")
        
        if quality_report.get('by_severity'):
            print(f"\nIssues by severity:")
            for severity, count in quality_report['by_severity'].items():
                print(f"  {severity}: {count}")
        
        if quality_report.get('by_type'):
            print(f"\nMost common issues:")
            sorted_types = sorted(quality_report['by_type'].items(), key=lambda x: x[1], reverse=True)
            for issue_type, count in sorted_types[:5]:
                print(f"  {issue_type}: {count}")
        
        print(f"\nCleaned data saved to data/processed/")
        print(f"Quality report saved to data/processed/quality_report.csv")
        
        # Show some examples of corrections
        corrected_hitting = cleaned_hitting[cleaned_hitting.get('stat_category_corrected', False) == True]
        if len(corrected_hitting) > 0:
            print(f"\nExample corrections in hitting data:")
            for _, record in corrected_hitting.head(5).iterrows():
                print(f"  {record['player_name']}: {record['stat_value']} -> {record['stat_category']}")
        
        corrected_pitching = cleaned_pitching[cleaned_pitching.get('stat_category_corrected', False) == True]
        if len(corrected_pitching) > 0:
            print(f"\nExample corrections in pitching data:")
            for _, record in corrected_pitching.head(5).iterrows():
                print(f"  {record['player_name']}: {record['stat_value']} -> {record['stat_category']}")
        
        return True
        
    except FileNotFoundError as e:
        print(f"Error: Could not find data files. Make sure to run the scraper first.")
        print(f"Missing file: {e}")
        return False
    except Exception as e:
        print(f"Error in data quality pipeline: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = run_data_quality_pipeline()
    if success:
        print("\nData quality pipeline completed successfully!")
    else:
        print("\nData quality pipeline failed - check error messages above")