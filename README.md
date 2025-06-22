# MLB Historical Data Analysis Project

**A complete data engineering pipeline for scraping, cleaning, and visualizing Major League Baseball historical data.**

## Project Overview

This project demonstrates a full data engineering workflow by scraping historical baseball data from Baseball Almanac, implementing data quality controls, storing data in a SQLite database, and creating an interactive dashboard for data visualization.

## Architecture

```
Web Scraping → Data Quality → SQLite Database → Interactive Dashboard
```

## Features

- Web scraping of MLB historical data (1882-2025)
- Data quality pipeline with automated cleaning and validation
- SQLite database with proper schema and relationships
- Command-line query tool with JOIN operations
- Interactive Streamlit dashboard with visualizations
- Production-ready code with logging and error handling

## Project Structure

```
baseball_project/
├── src/                          # Source code
│   ├── scraper.py               # Web scraping program
│   ├── data_quality.py          # Data cleaning and validation
│   ├── db_import.py             # Database import program
│   ├── db_query.py              # Command-line query tool
│   ├── dashboard.py             # Streamlit dashboard
│   └── view_database.py         # Database viewer utility
├── data/                        # Data files
│   ├── raw/                     # Raw scraped data (CSV)
│   └── processed/               # Cleaned data (CSV)
├── database/                    # SQLite database
│   └── baseball.db             # Main database file
├── requirements.txt            # Python dependencies
├── README.md                   # This file
├── SQL_QUERIES.md             # SQL query examples
└── DATA_QUALITY_REPORT.md     # Data quality documentation
```

## Quick Start

### Prerequisites

- Python 3.8+
- Git

### Installation

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd baseball_project
   ```

2. **Create virtual environment**
   ```bash
   python -m venv venv
   source venv/Scripts/activate  # Windows Git Bash
   # or
   venv\Scripts\activate.bat     # Windows CMD
   ```

3. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

### Usage

#### 1. Scrape Data
```bash
python src/scraper.py
```
- Scrapes MLB historical data from Baseball Almanac
- Creates CSV files in `data/raw/`
- Includes hitting leaders, pitching leaders, and team standings

#### 2. Clean Data (Optional)
```bash
python src/data_quality.py
```
- Validates and cleans scraped data
- Fixes statistical category misclassifications
- Generates quality report in `data/processed/`

#### 3. Import to Database
```bash
python src/db_import.py
```
- Creates SQLite database with proper schema
- Imports cleaned data with relationships
- Includes data quality audit trail

#### 4. Query Database
```bash
# Interactive mode
python src/db_query.py --interactive

# Show tables
python src/db_query.py --tables

# Custom query
python src/db_query.py --query "SELECT * FROM hitting_leaders WHERE stat_category = 'Home Runs' ORDER BY stat_value DESC LIMIT 5"
```

#### 5. Launch Dashboard
```bash
streamlit run src/dashboard.py
```
- Interactive web dashboard at http://localhost:8501
- Multiple visualizations and data exploration tools
- Demonstrates JOIN operations between tables

## Data Overview

### Data Sources
- **Baseball Almanac** (https://www.baseball-almanac.com)
- **Coverage**: 1882-2025 (135+ years of data)
- **Data Types**: Hitting statistics, pitching statistics, team standings

### Database Schema

#### Tables
- **hitting_leaders**: Player hitting statistics by year
- **pitching_leaders**: Player pitching statistics by year  
- **team_standings**: Team win-loss records by year
- **data_quality_log**: Data quality audit trail

#### Key Fields
- Player names, teams, years, statistical categories
- Quality scores and validation flags
- Standardized team names and corrected statistics

## Sample Queries

### Basic Queries
```sql
-- Top home run hitters
SELECT player_name, team, stat_value, year 
FROM hitting_leaders 
WHERE stat_category = 'Home Runs' 
ORDER BY stat_value DESC LIMIT 10;

-- Team performance over time
SELECT year, team_name, wins, losses, win_pct 
FROM team_standings 
ORDER BY win_pct DESC;
```

### JOIN Queries (Project Requirement)
```sql
-- Players with team performance
SELECT h.player_name, h.stat_value as home_runs, 
       t.team_name, t.wins, t.win_pct
FROM hitting_leaders h
JOIN team_standings t ON h.year = t.year
WHERE h.stat_category = 'Home Runs'
ORDER BY h.stat_value DESC;
```

See [SQL_QUERIES.md](SQL_QUERIES.md) for more examples.

## Dashboard Features

### Interactive Visualizations
- **Home Run Leaders**: Bar charts with year filtering
- **Team Performance**: Line/bar charts showing win percentages over time
- **JOIN Analysis**: Scatter plots correlating player performance with team success
- **Data Quality**: Distribution charts and issue tracking
- **Raw Data Explorer**: Searchable data tables

### User Controls
- Year filtering (1882-2025)
- Data type selection
- Interactive hover details
- Responsive design

## Technical Details

### Technologies Used
- **Python 3.8+**: Core programming language
- **Requests + BeautifulSoup**: Web scraping (chosen over Selenium for static content)
- **Pandas**: Data manipulation and analysis
- **SQLite**: Embedded database for data storage
- **Streamlit**: Interactive web dashboard framework
- **Plotly**: Interactive data visualizations

### Data Quality Pipeline
- **Validation Rules**: Historical ranges for baseball statistics
- **Auto-Correction**: Fixes misclassified statistical categories
- **Quality Scoring**: 0-100 quality score for each record
- **Audit Trail**: Complete log of all data quality issues

### Performance
- **Database Size**: ~106KB (compact SQLite file)
- **Record Count**: 70+ player records, 12 team standings
- **Query Performance**: Indexed for fast lookups
- **Dashboard Load Time**: <2 seconds with caching

## Project Requirements Compliance

**Web Scraping Program**: `src/scraper.py`  
**Data Cleaning**: `src/data_quality.py`  
**SQLite Database**: `database/baseball.db`  
**Database Import**: `src/db_import.py`  
**Command-line Queries with JOINs**: `src/db_query.py`  
**Interactive Dashboard**: `src/dashboard.py`  

## Data Quality Report

The data quality pipeline identified and corrected 60 issues across the dataset:

- **30 misclassifications**: Statistical categories corrected (e.g., 137 "Home Runs" → "Hits")
- **18 out-of-range values**: Values outside expected ranges
- **10 missing values**: Incomplete records flagged
- **2 suspicious values**: Unusually high but valid statistics

**Key Corrections:**
- Babe Ruth 1927: Correctly shows 60 home runs (historical record)
- Statistical categories properly classified using domain knowledge
- Team names standardized (e.g., "New York" → "New York Yankees")

See [DATA_QUALITY_REPORT.md](DATA_QUALITY_REPORT.md) for detailed analysis.

## Testing

### Manual Testing
```bash
# Test scraper
python src/scraper.py

# Test database
python src/view_database.py

# Test queries
python src/db_query.py --query "SELECT COUNT(*) FROM hitting_leaders"

# Test dashboard
streamlit run src/dashboard.py
```

### Expected Results
- Scraper: Creates 3 CSV files with 100+ records
- Database: 4 tables with proper relationships
- Queries: Fast execution with accurate results
- Dashboard: Loads in <2 seconds, all visualizations work

## Deployment

### Local Development
- All components run locally
- SQLite database requires no server setup
- Streamlit dashboard accessible at localhost:8501

### Production Considerations
- Dashboard can be deployed to Streamlit Cloud
- Database can be migrated to PostgreSQL for larger datasets
- Scraper can be scheduled with cron/Task Scheduler
- Add authentication for multi-user environments

## Contributing

### Development Setup
1. Fork the repository
2. Create feature branch
3. Make changes with tests
4. Submit pull request

### Code Style
- Use descriptive variable names
- Add docstrings to functions
- Follow PEP 8 guidelines
- Include error handling

## License

This project is for educational purposes. Baseball data is publicly available from Baseball Almanac.

## Contact

For questions about this project, please refer to the documentation or create an issue in the repository.

---

**Data Pipeline:** Web Scraping → Data Quality → SQLite Database → Interactive Dashboard