# SQL Query Examples

This document contains SQL query examples for the MLB Historical Data database. All queries can be executed using the command-line query tool or directly in SQLite.

## Running Queries

### Using the Command-line Tool
```bash
# Interactive mode
python src/db_query.py --interactive

# Single query
python src/db_query.py --query "SELECT * FROM hitting_leaders LIMIT 5"

# Show tables
python src/db_query.py --tables
```

### Direct SQLite Access
```bash
sqlite3 database/baseball.db
```

## Database Schema

### Tables Overview
- **hitting_leaders**: Player hitting statistics by year
- **pitching_leaders**: Player pitching statistics by year
- **team_standings**: Team win-loss records by year
- **data_quality_log**: Data quality audit trail

### Table Structures

#### hitting_leaders
```sql
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
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

#### team_standings
```sql
CREATE TABLE team_standings (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    year INTEGER NOT NULL,
    team_name TEXT NOT NULL,
    wins INTEGER NOT NULL,
    losses INTEGER NOT NULL,
    win_pct REAL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

## Basic Queries

### 1. Simple SELECT Queries

#### All Babe Ruth records
```sql
SELECT * FROM hitting_leaders 
WHERE player_name = 'Babe Ruth';
```

#### Top 10 home run hitters
```sql
SELECT player_name, team, stat_value, year
FROM hitting_leaders 
WHERE stat_category = 'Home Runs' 
ORDER BY stat_value DESC 
LIMIT 10;
```

#### Teams with best win percentages
```sql
SELECT year, team_name, wins, losses, win_pct
FROM team_standings 
ORDER BY win_pct DESC
LIMIT 10;
```

### 2. Filtering and Conditions

#### Players with more than 50 home runs
```sql
SELECT player_name, team, stat_value, year
FROM hitting_leaders 
WHERE stat_category = 'Home Runs' 
  AND stat_value > 50
ORDER BY stat_value DESC;
```

#### Teams with 100+ wins
```sql
SELECT year, team_name, wins, losses, win_pct
FROM team_standings 
WHERE wins >= 100
ORDER BY wins DESC;
```

#### High-quality data records
```sql
SELECT player_name, stat_category, stat_value, quality_score
FROM hitting_leaders 
WHERE quality_level = 'high'
  AND quality_score > 95
ORDER BY quality_score DESC;
```

### 3. Aggregate Functions

#### Statistics by year
```sql
SELECT 
    year,
    COUNT(*) as total_records,
    AVG(stat_value) as avg_stat_value,
    MAX(stat_value) as max_stat_value
FROM hitting_leaders 
WHERE stat_category = 'Home Runs'
GROUP BY year 
ORDER BY year DESC;
```

#### Quality distribution
```sql
SELECT 
    quality_level,
    COUNT(*) as record_count,
    AVG(quality_score) as avg_quality_score
FROM hitting_leaders 
GROUP BY quality_level
ORDER BY avg_quality_score DESC;
```

#### Team performance summary
```sql
SELECT 
    COUNT(*) as total_seasons,
    AVG(wins) as avg_wins,
    AVG(win_pct) as avg_win_pct,
    MAX(wins) as best_season_wins
FROM team_standings;
```

## JOIN Queries (Project Requirement)

### 1. Basic Joins

#### Players with their team's performance
```sql
SELECT 
    h.player_name,
    h.team,
    h.stat_value as home_runs,
    t.team_name,
    t.wins,
    t.losses,
    t.win_pct
FROM hitting_leaders h
JOIN team_standings t ON h.year = t.year
WHERE h.stat_category = 'Home Runs'
ORDER BY h.stat_value DESC;
```

#### Yankees players with team standings
```sql
SELECT 
    h.year,
    h.player_name,
    h.stat_category,
    h.stat_value,
    t.wins,
    t.losses,
    t.win_pct
FROM hitting_leaders h
JOIN team_standings t ON h.year = t.year
WHERE h.team LIKE '%Yankees%' 
  AND t.team_name LIKE '%Yankees%'
ORDER BY h.year DESC, h.stat_value DESC;
```

### 2. Advanced Joins

#### Best hitters and pitchers by year
```sql
SELECT 
    h.year,
    h.player_name as best_hitter,
    h.stat_value as home_runs,
    p.player_name as best_pitcher,
    p.stat_value as era
FROM 
    (SELECT year, player_name, stat_value,
            ROW_NUMBER() OVER (PARTITION BY year ORDER BY stat_value DESC) as rn
     FROM hitting_leaders WHERE stat_category = 'Home Runs') h
JOIN 
    (SELECT year, player_name, stat_value,
            ROW_NUMBER() OVER (PARTITION BY year ORDER BY stat_value ASC) as rn
     FROM pitching_leaders WHERE stat_category = 'ERA') p
ON h.year = p.year AND h.rn = 1 AND p.rn = 1
ORDER BY h.year DESC;
```

#### Team success correlation with star players
```sql
SELECT 
    t.year,
    t.team_name,
    t.wins,
    t.win_pct,
    COUNT(h.player_name) as star_players,
    AVG(h.stat_value) as avg_home_runs
FROM team_standings t
LEFT JOIN hitting_leaders h ON t.year = h.year
    AND h.stat_category = 'Home Runs'
    AND h.stat_value > 40  -- High performance threshold
GROUP BY t.year, t.team_name, t.wins, t.win_pct
HAVING star_players > 0
ORDER BY t.win_pct DESC;
```

### 3. Multi-table Joins

#### Complete player performance with quality metrics
```sql
SELECT 
    h.player_name,
    h.year,
    h.stat_value as home_runs,
    t.team_name,
    t.win_pct,
    h.quality_score,
    COUNT(q.record_id) as quality_issues
FROM hitting_leaders h
JOIN team_standings t ON h.year = t.year
LEFT JOIN data_quality_log q ON q.record_id LIKE h.year || '-' || h.player_name || '%'
WHERE h.stat_category = 'Home Runs'
GROUP BY h.player_name, h.year, h.stat_value, t.team_name, t.win_pct, h.quality_score
ORDER BY h.stat_value DESC;
```

## Subqueries and Advanced Analysis

### 1. Performance Above Average

#### Players better than average
```sql
SELECT player_name, team, stat_value, year
FROM hitting_leaders 
WHERE stat_category = 'Home Runs'
  AND stat_value > (
    SELECT AVG(stat_value) 
    FROM hitting_leaders 
    WHERE stat_category = 'Home Runs'
  )
ORDER BY stat_value DESC;
```

#### Teams better than historical average
```sql
SELECT year, team_name, wins, win_pct
FROM team_standings
WHERE win_pct > (
    SELECT AVG(win_pct) 
    FROM team_standings
  )
ORDER BY win_pct DESC;
```

### 2. Ranking and Window Functions

#### Player rankings by year
```sql
SELECT 
    year,
    player_name,
    stat_value,
    RANK() OVER (PARTITION BY year ORDER BY stat_value DESC) as rank,
    DENSE_RANK() OVER (PARTITION BY year ORDER BY stat_value DESC) as dense_rank
FROM hitting_leaders 
WHERE stat_category = 'Home Runs'
ORDER BY year DESC, rank;
```

#### Running totals and moving averages
```sql
SELECT 
    year,
    team_name,
    wins,
    SUM(wins) OVER (PARTITION BY team_name ORDER BY year) as cumulative_wins,
    AVG(wins) OVER (PARTITION BY team_name ORDER BY year ROWS 2 PRECEDING) as three_year_avg
FROM team_standings
WHERE team_name = 'New York Yankees'
ORDER BY year;
```

## Data Quality Queries

### 1. Quality Analysis

#### Records with corrections
```sql
SELECT player_name, stat_category, stat_value, quality_score
FROM hitting_leaders 
WHERE stat_category_corrected = 1
ORDER BY quality_score;
```

#### Quality issues by type
```sql
SELECT 
    issue_type,
    severity,
    COUNT(*) as issue_count
FROM data_quality_log 
GROUP BY issue_type, severity
ORDER BY issue_count DESC;
```

#### Quality trends by table
```sql
SELECT 
    table_name,
    severity,
    COUNT(*) as issues
FROM data_quality_log
GROUP BY table_name, severity
ORDER BY table_name, issues DESC;
```

### 2. Data Validation

#### Suspicious statistical values
```sql
SELECT player_name, stat_category, stat_value, quality_level
FROM hitting_leaders 
WHERE quality_level IN ('medium', 'low')
ORDER BY quality_score;
```

#### Missing or incomplete data
```sql
SELECT 
    'hitting_leaders' as table_name,
    COUNT(*) as total_records,
    SUM(CASE WHEN player_name IS NULL OR player_name = '' THEN 1 ELSE 0 END) as missing_names,
    SUM(CASE WHEN team IS NULL OR team = '' THEN 1 ELSE 0 END) as missing_teams
FROM hitting_leaders
UNION ALL
SELECT 
    'team_standings' as table_name,
    COUNT(*) as total_records,
    SUM(CASE WHEN team_name IS NULL OR team_name = '' THEN 1 ELSE 0 END) as missing_names,
    SUM(CASE WHEN wins IS NULL THEN 1 ELSE 0 END) as missing_wins
FROM team_standings;
```

## Performance Optimization

### 1. Using Indexes

The database includes these indexes for better performance:

```sql
-- Show all indexes
SELECT name, sql FROM sqlite_master WHERE type = 'index';

-- Example indexes in the database:
CREATE INDEX idx_hitting_year ON hitting_leaders(year);
CREATE INDEX idx_hitting_player ON hitting_leaders(player_name);
CREATE INDEX idx_hitting_stat ON hitting_leaders(stat_category);
```

### 2. Query Optimization Tips

#### Use LIMIT for large results
```sql
SELECT * FROM hitting_leaders 
ORDER BY stat_value DESC 
LIMIT 100;  -- Only return top 100
```

#### Filter early in WHERE clause
```sql
-- Good: Filter first, then sort
SELECT * FROM hitting_leaders 
WHERE stat_category = 'Home Runs' 
  AND year >= 2000
ORDER BY stat_value DESC;
```

#### Use appropriate JOIN types
```sql
-- INNER JOIN when you need matching records only
-- LEFT JOIN when you want to keep all records from left table
-- Use EXISTS for checking existence rather than IN with subqueries
```

## Practical Examples

### 1. Historical Analysis

#### Greatest seasons in baseball history
```sql
SELECT 
    h.year,
    h.player_name,
    h.stat_value as home_runs,
    t.team_name,
    t.wins,
    t.win_pct
FROM hitting_leaders h
JOIN team_standings t ON h.year = t.year
WHERE h.stat_category = 'Home Runs'
  AND h.stat_value >= 50  -- 50+ home run seasons
  AND t.wins >= 100       -- 100+ win teams
ORDER BY h.stat_value DESC;
```

#### Team dynasty analysis
```sql
SELECT 
    team_name,
    COUNT(*) as championship_caliber_seasons,
    AVG(wins) as avg_wins,
    AVG(win_pct) as avg_win_pct
FROM team_standings
WHERE win_pct >= 0.600  -- 60%+ win rate
GROUP BY team_name
HAVING championship_caliber_seasons >= 2
ORDER BY avg_win_pct DESC;
```

### 2. Data Pipeline Monitoring

#### Data completeness check
```sql
SELECT 
    'Data Pipeline Health Check' as check_type,
    (SELECT COUNT(*) FROM hitting_leaders) as hitting_records,
    (SELECT COUNT(*) FROM pitching_leaders) as pitching_records,
    (SELECT COUNT(*) FROM team_standings) as standings_records,
    (SELECT COUNT(*) FROM data_quality_log) as quality_issues;
```

#### Quality score distribution
```sql
SELECT 
    CASE 
        WHEN quality_score >= 90 THEN 'Excellent (90-100)'
        WHEN quality_score >= 80 THEN 'Good (80-89)'
        WHEN quality_score >= 70 THEN 'Fair (70-79)'
        ELSE 'Poor (<70)'
    END as quality_tier,
    COUNT(*) as record_count,
    ROUND(AVG(quality_score), 2) as avg_score
FROM hitting_leaders
GROUP BY 
    CASE 
        WHEN quality_score >= 90 THEN 'Excellent (90-100)'
        WHEN quality_score >= 80 THEN 'Good (80-89)'
        WHEN quality_score >= 70 THEN 'Fair (70-79)'
        ELSE 'Poor (<70)'
    END
ORDER BY avg_score DESC;
```

## Query Performance Notes

- All queries include appropriate LIMIT clauses for large result sets
- Indexes are used on frequently queried columns (year, player_name, stat_category)
- JOIN operations are optimized for the database size and structure
- Complex analytical queries use window functions where appropriate
- Quality queries help monitor data pipeline health

For more complex analysis, consider exporting data to pandas DataFrames using the Python tools provided in the project.