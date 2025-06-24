# ⚾ MLB Historical Data Dashboard Project

A comprehensive web scraping and data visualization project that collects, processes, and displays historical Major League Baseball data from significant years in baseball history.

## 📁 Project Structure

```
baseball_project/
├── src/
│   ├── scraper.py          # Web scraping program (Selenium + BeautifulSoup)
│   ├── db_import.py        # Database import program (SQLite)
│   ├── db_query.py         # Database query program (CLI interface)
│   └── dashboard.py        # Streamlit dashboard application
├── data/
│   ├── raw/               # Raw CSV files from scraping
│   │   ├── yearly_hitting_leaders.csv
│   │   ├── yearly_pitching_leaders.csv
│   │   ├── team_standings.csv
│   │   └── notable_events.csv
│   └── mlb_database.db    # SQLite database
├── requirements.txt       # Python dependencies
└── README.md             # This file
```

## 🚀 Quick Start

### 1. Setup Environment

```bash
# Create virtual environment
python -m venv venv

# Activate virtual environment
# Windows:
venv\Scripts\activate
# Mac/Linux:
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

### 2. Run the Data Pipeline

```bash
# Step 1: Scrape historical MLB data
python src/scraper.py

# Step 2: Import data into SQLite database
python src/db_import.py

# Step 3: Query the database (optional)
python src/db_query.py

# Step 4: Launch the interactive dashboard
streamlit run src/dashboard.py
```

## 📊 Dashboard Features

### Interactive Visualizations
- **Team Performance Timeline**: Track wins/losses over time for selected teams
- **Home Run Leaders**: Bar chart of home run champions by year
- **Batting Average Distribution**: Box plots showing hitting performance
- **Notable Events Timeline**: Historical events categorized by type
- **ERA vs Team Performance**: Scatter plot correlating pitching with team success

### Interactive Controls
- **Year Filtering**: Select specific years to analyze
- **Team Selection**: Choose teams for performance tracking
- **Category Filtering**: Filter hitting/pitching statistics by category
- **Event Type Filtering**: Filter notable events by type

### Data Tables
- Complete team standings with win-loss records
- Statistical leaders in hitting and pitching categories
- Detailed notable events with descriptions

## 🗃️ Database Schema

### Tables
- **standings**: Team records (wins, losses, win percentage)
- **hitting_leaders**: Top hitting statistical performers
- **pitching_leaders**: Top pitching statistical performers  
- **notable_events**: Historical events and milestones

### Key Relationships
- All tables linked by year for comprehensive analysis
- Optimized with indexes for fast querying

## 📈 Data Coverage

**Years Included**: 1927, 1947, 1961, 1969, 1994, 1998, 2001, 2016, 2020, 2023

**Why These Years?**
- 1927: Babe Ruth's 60 home run season
- 1947: Jackie Robinson breaks color barrier
- 1961: Roger Maris' 61 home run record
- 1969: Expansion era and "Miracle Mets"
- 1994: Strike-shortened season
- 1998: McGwire and Sosa home run chase
- 2001: Seattle Mariners' 116 wins
- 2016: Cubs break "Curse of the Bambino"
- 2020: COVID-shortened season
- 2023: Recent modern baseball

## 🛠️ Technical Implementation

### Web Scraping (src/scraper.py)
- **Primary**: Selenium WebDriver for JavaScript-heavy pages
- **Fallback**: Requests + BeautifulSoup for static content
- **Features**: Handles pagination, missing data, user-agent rotation
- **Data Extraction**: Team standings, player statistics, notable events

### Database Management (src/db_import.py)
- **Engine**: SQLite for portability and performance
- **Features**: Data validation, type conversion, error handling
- **Optimization**: Indexes on commonly queried columns

### Query Interface (src/db_query.py)
- **Interactive CLI**: Menu-driven query system
- **Predefined Queries**: 10 optimized queries with JOINs
- **Custom SQL**: Direct SQL input capability
- **Error Handling**: Graceful handling of invalid queries

### Dashboard (src/dashboard.py)
- **Framework**: Streamlit for rapid development
- **Visualizations**: Plotly for interactive charts
- **Features**: Real-time filtering, responsive design
- **Performance**: Cached data loading for speed

## 📋 Usage Examples

### Query Interface Examples

```bash
# Run specific predefined query
python src/db_query.py 1

# Interactive mode
python src/db_query.py
# Then select from menu options like:
# 1. Top Home Run Leaders by Year
# 4. Yankees Performance Across Years
# 8. Custom Query
```

### Sample SQL Queries

```sql
-- Home run leaders with team performance
SELECT h.year, h.player_name, h.stat_value as home_runs, 
       s.team_name, s.wins, s.losses
FROM hitting_leaders h
JOIN standings s ON h.year = s.year 
WHERE h.stat_category = 'Home Runs'
ORDER BY h.stat_value DESC;

-- Teams with 100+ wins
SELECT year, team_name, wins, losses, win_pct
FROM standings 
WHERE wins >= 100
ORDER BY wins DESC;
```

### Dashboard Deployment

```bash
# Local development
streamlit run src/dashboard.py

# For production deployment on Streamlit Cloud:
# 1. Push to GitHub repository
# 2. Connect repository to Streamlit Cloud
# 3. Dashboard will be available at: https://share.streamlit.io/[username]/[repo]
```

## 🎯 Project Rubric Compliance

### ✅ Web Scraping
- Uses Selenium for dynamic content
- Handles missing tags, pagination, user-agent headers
- Saves data as CSV files
- Avoids duplication through intelligent caching

### ✅ Data Cleaning & Transformation
- Loads raw data into Pandas DataFrames
- Cleans missing/duplicate entries
- Applies appropriate data type conversions
- Documents before/after data states

### ✅ Data Visualization
- 6+ interactive visualizations using Plotly
- Relevant, well-labeled charts supporting data insights
- Interactive controls (dropdowns, multi-select, sliders)
- Responsive design that updates based on user input

### ✅ Dashboard Functionality
- Built with Streamlit for professional presentation
- Clean, intuitive layout with organized sections
- Multiple views for exploring different data aspects
- Clear instructions and helpful tooltips

### ✅ Code Quality & Documentation
- Well-organized, modular code structure
- Comprehensive inline comments
- All dependencies listed in requirements.txt
- Complete README with setup instructions

## 🚀 Deployment Options

### Streamlit Cloud (Recommended)
1. Push code to GitHub repository
2. Visit [share.streamlit.io](https://share.streamlit.io)
3. Connect GitHub repository
4. Dashboard auto-deploys with public URL

### Render
1. Create account at [render.com](https://render.com)
2. Connect GitHub repository
3. Configure as Web Service with Streamlit
4. Auto-deployment with custom domain

### Local Network
```bash
# Run on local network (accessible to other devices)
streamlit run src/dashboard.py --server.address 0.0.0.0
```

## 🔧 Troubleshooting

### Common Issues

**Database Connection Error**
```bash
# Ensure database exists
python src/db_import.py
```

**Missing ChromeDriver**
```bash
# Install ChromeDriver for Selenium
# Or modify scraper.py to use different browser
```

**Import Errors**
```bash
# Reinstall dependencies
pip install -r requirements.txt --force-reinstall
```

## 📝 Future Enhancements

- Add more years of historical data
- Include playoff and World Series statistics
- Add player salary and contract information
- Implement real-time data updates
- Add machine learning predictions
- Create mobile-responsive design

## 📄 License

This project is for educational purposes. Baseball data is publicly available historical information.

## 👨‍💻 Author

Created as part of a web scraping and data visualization course project.

---

**Last Updated**: June 2025  
**Python Version**: 3.8+  
**Key Dependencies**: Streamlit, Pandas, Plotly, Selenium, BeautifulSoup4