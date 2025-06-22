import streamlit as st
import pandas as pd
import sqlite3
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import numpy as np

# Page configuration
st.set_page_config(
    page_title="MLB Historical Data Dashboard",
    page_icon="⚾",
    layout="wide",
    initial_sidebar_state="expanded"
)

@st.cache_data
def load_data():
    """Load data from SQLite database with caching"""
    try:
        conn = sqlite3.connect("database/baseball.db")
        
        # Load hitting leaders
        hitting_df = pd.read_sql_query("""
            SELECT year, player_name, team, stat_category, stat_value, 
                   quality_score, quality_level
            FROM hitting_leaders
            ORDER BY year, stat_value DESC
        """, conn)
        
        # Load pitching leaders  
        pitching_df = pd.read_sql_query("""
            SELECT year, player_name, team, stat_category, stat_value,
                   quality_score, quality_level
            FROM pitching_leaders
            ORDER BY year, stat_value
        """, conn)
        
        # Load team standings
        standings_df = pd.read_sql_query("""
            SELECT year, team_name, wins, losses, win_pct
            FROM team_standings
            ORDER BY year DESC, wins DESC
        """, conn)
        
        # Load quality data
        quality_df = pd.read_sql_query("""
            SELECT record_id, table_name, field_name, issue_type, 
                   description, severity
            FROM data_quality_log
        """, conn)
        
        conn.close()
        return hitting_df, pitching_df, standings_df, quality_df
        
    except Exception as e:
        st.error(f"Error loading data: {e}")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

def create_home_run_chart(hitting_df):
    """Create interactive home run leaders chart"""
    hr_data = hitting_df[hitting_df['stat_category'] == 'Home Runs'].copy()
    
    if hr_data.empty:
        st.warning("No home run data available")
        return None
    
    # Create bar chart
    fig = px.bar(
        hr_data.head(10),
        x='player_name',
        y='stat_value',
        color='year',
        title='Top Home Run Leaders',
        labels={'stat_value': 'Home Runs', 'player_name': 'Player'},
        text='stat_value'
    )
    
    fig.update_traces(texttemplate='%{text}', textposition='outside')
    fig.update_layout(xaxis_tickangle=-45, height=500)
    
    return fig

def create_team_performance_chart(standings_df):
    """Create team performance over time - FIXED for single years"""
    if standings_df.empty:
        st.warning("No team standings data available")
        return None
    
    # Check if we have multiple years for line chart
    unique_years = standings_df['year'].nunique()
    
    if unique_years > 1:
        # Multiple years - use line chart
        fig = px.line(
            standings_df,
            x='year',
            y='win_pct',
            color='team_name',
            title='Team Win Percentage Over Time',
            labels={'win_pct': 'Win Percentage', 'year': 'Year'},
            markers=True  # Add markers to make single points visible
        )
    else:
        # Single year - use bar chart instead
        fig = px.bar(
            standings_df,
            x='team_name',
            y='win_pct',
            color='team_name',
            title=f'Team Win Percentage - {standings_df["year"].iloc[0]}',
            labels={'win_pct': 'Win Percentage', 'team_name': 'Team'},
            text='win_pct'
        )
        fig.update_traces(texttemplate='%{text:.3f}', textposition='outside')
        fig.update_layout(xaxis_tickangle=-45, showlegend=False)
    
    fig.update_layout(height=500)
    return fig

def create_quality_metrics_chart(hitting_df, pitching_df):
    """Create data quality visualization"""
    # Combine quality data
    hitting_quality = hitting_df[['quality_level']].copy()
    hitting_quality['table'] = 'Hitting'
    
    pitching_quality = pitching_df[['quality_level']].copy()  
    pitching_quality['table'] = 'Pitching'
    
    combined_quality = pd.concat([hitting_quality, pitching_quality])
    
    if combined_quality.empty:
        st.warning("No quality data available")
        return None
    
    # Create quality distribution chart
    quality_counts = combined_quality.groupby(['table', 'quality_level']).size().reset_index(name='count')
    
    fig = px.bar(
        quality_counts,
        x='table',
        y='count',
        color='quality_level',
        title='Data Quality Distribution',
        labels={'count': 'Number of Records'}
    )
    
    return fig

def create_player_team_join_analysis(hitting_df, standings_df):
    """Create analysis joining player stats with team performance - FIXED"""
    # Get home run leaders
    hr_leaders = hitting_df[hitting_df['stat_category'] == 'Home Runs'].copy()
    
    if hr_leaders.empty or standings_df.empty:
        st.warning("Insufficient data for join analysis")
        return None, None
    
    # FIXED: Simpler team matching
    merged_data = []
    
    for _, hr_row in hr_leaders.iterrows():
        # Simple matching by year (without complex team name matching)
        team_matches = standings_df[standings_df['year'] == hr_row['year']]
        
        if not team_matches.empty:
            # Take first matching team for JOIN demonstration
            team_row = team_matches.iloc[0]
            merged_data.append({
                'player_name': hr_row['player_name'],
                'year': hr_row['year'],
                'home_runs': hr_row['stat_value'],
                'team': hr_row['team'],
                'team_wins': team_row['wins'],
                'team_win_pct': team_row['win_pct'],
                'team_name': team_row['team_name']
            })
    
    if not merged_data:
        st.warning("No matching data found between players and team standings")
        return None, None
    
    merged_df = pd.DataFrame(merged_data)
    
    # Create scatter plot (works for any number of points)
    fig = px.scatter(
        merged_df,
        x='home_runs',
        y='team_win_pct',
        size='team_wins',
        color='year',
        hover_data=['player_name', 'team', 'team_name'],
        title='Home Runs vs Team Performance (JOIN Analysis)',
        labels={'home_runs': 'Player Home Runs', 'team_win_pct': 'Team Win Percentage'}
    )
    
    fig.update_layout(height=400)
    
    return fig, merged_df

def main():
    """Main dashboard function"""
    
    # Header
    st.title("⚾ MLB Historical Data Dashboard")
    st.markdown("Interactive visualization of scraped and cleaned baseball data")
    
    # Load data
    with st.spinner("Loading data from SQLite database..."):
        hitting_df, pitching_df, standings_df, quality_df = load_data()
    
    if hitting_df.empty and pitching_df.empty and standings_df.empty:
        st.error("No data available. Please run the scraper and database import first.")
        st.code("""
        # Run these commands first:
        python src/scraper.py
        python src/data_quality.py  
        python src/db_import.py
        """)
        return
    
    # Sidebar
    st.sidebar.header("Dashboard Controls")
    
    # Data overview in sidebar
    st.sidebar.subheader("Data Overview")
    st.sidebar.metric("Hitting Records", len(hitting_df))
    st.sidebar.metric("Pitching Records", len(pitching_df))
    st.sidebar.metric("Team Standings", len(standings_df))
    st.sidebar.metric("Quality Issues", len(quality_df))
    
    # Year filter
    years_available = sorted(hitting_df['year'].unique()) if not hitting_df.empty else []
    if years_available:
        selected_years = st.sidebar.multiselect(
            "Select Years",
            years_available,
            default=None,  # Start with no years selected
            help="Select years to filter data. Leave empty to show instructions."
        )
    else:
        selected_years = []
    
    # Main content tabs
    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "🏠 Home Run Leaders", 
        "🏆 Team Performance", 
        "🔗 Player-Team Analysis (JOIN)", 
        "📊 Data Quality",
        "🗃️ Raw Data"
    ])
    
    # FIXED data filtering by selected years
    if selected_years:  # If years are selected
        hitting_filtered = hitting_df[hitting_df['year'].isin(selected_years)]
        pitching_filtered = pitching_df[pitching_df['year'].isin(selected_years)]
        standings_filtered = standings_df[standings_df['year'].isin(selected_years)]
    else:  # If NO years selected - show empty data
        hitting_filtered = pd.DataFrame()
        pitching_filtered = pd.DataFrame()
        standings_filtered = pd.DataFrame()
    
    # Tab 1: Home Run Leaders
    with tab1:
        st.header("Home Run Leaders")
        
        if hitting_filtered.empty:
            st.info("Please select years from the sidebar to view data")
        else:
            col1, col2 = st.columns([2, 1])
            
            with col1:
                # Home run chart
                hr_fig = create_home_run_chart(hitting_filtered)
                if hr_fig:
                    st.plotly_chart(hr_fig, use_container_width=True)
            
            with col2:
                # Top performers table
                hr_data = hitting_filtered[hitting_filtered['stat_category'] == 'Home Runs']
                if not hr_data.empty:
                    st.subheader("Top 5 Home Run Hitters")
                    top_hr = hr_data.nlargest(5, 'stat_value')[['player_name', 'year', 'team', 'stat_value']]
                    st.dataframe(top_hr, hide_index=True)
                    
                    # Highlight Babe Ruth if present
                    babe_ruth = hr_data[hr_data['player_name'] == 'Babe Ruth']
                    if not babe_ruth.empty:
                        st.success(f"⭐ Babe Ruth: {babe_ruth.iloc[0]['stat_value']} home runs ({babe_ruth.iloc[0]['year']})")
    
    # Tab 2: Team Performance
    with tab2:
        st.header("Team Performance Over Time")
        
        if standings_filtered.empty:
            st.info("Please select years from the sidebar to view team data")
        else:
            # Team performance chart
            team_fig = create_team_performance_chart(standings_filtered)
            if team_fig:
                st.plotly_chart(team_fig, use_container_width=True)
            
            # Best teams table
            col1, col2 = st.columns(2)
            
            with col1:
                st.subheader("Best Win Percentages")
                best_teams = standings_filtered.nlargest(5, 'win_pct')[['year', 'team_name', 'wins', 'losses', 'win_pct']]
                st.dataframe(best_teams, hide_index=True)
            
            with col2:
                st.subheader("Most Wins")
                most_wins = standings_filtered.nlargest(5, 'wins')[['year', 'team_name', 'wins', 'losses', 'win_pct']]
                st.dataframe(most_wins, hide_index=True)
    
    # Tab 3: JOIN Analysis
    with tab3:
        st.header("Player Performance vs Team Success (JOIN Analysis)")
        st.markdown("This demonstrates JOIN queries between hitting_leaders and team_standings tables")
        
        if hitting_filtered.empty or standings_filtered.empty:
            st.info("Please select years from the sidebar to view JOIN analysis")
        else:
            join_fig, join_data = create_player_team_join_analysis(hitting_filtered, standings_filtered)
            
            if join_fig and join_data is not None:
                st.plotly_chart(join_fig, use_container_width=True)
                
                st.subheader("JOIN Query Results")
                st.dataframe(join_data, hide_index=True)
                
                # Show the actual SQL
                st.subheader("SQL JOIN Query Used")
                st.code("""
                SELECT 
                    h.player_name,
                    h.year,
                    h.stat_value as home_runs,
                    h.team,
                    t.wins as team_wins,
                    t.win_pct as team_win_pct
                FROM hitting_leaders h
                JOIN team_standings t ON h.year = t.year
                WHERE h.stat_category = 'Home Runs'
                ORDER BY h.stat_value DESC
                """, language="sql")
    
    # Tab 4: Data Quality
    with tab4:
        st.header("Data Quality Metrics")
        st.info("📊 Data Quality metrics show analysis for the complete dataset regardless of year selection")
        
        # Use original data for quality overview
        col1, col2 = st.columns(2)
        
        with col1:
            # Quality distribution chart
            quality_fig = create_quality_metrics_chart(hitting_df, pitching_df)
            if quality_fig:
                st.plotly_chart(quality_fig, use_container_width=True)
        
        with col2:
            # Quality issues summary
            if not quality_df.empty:
                st.subheader("Quality Issues by Type")
                issue_counts = quality_df['issue_type'].value_counts()
                st.bar_chart(issue_counts)
                
                st.subheader("Severity Distribution")
                severity_counts = quality_df['severity'].value_counts()
                st.write(severity_counts)
        
        # Quality issues table
        if not quality_df.empty:
            st.subheader("Recent Quality Issues")
            st.dataframe(quality_df.head(10), hide_index=True)
    
    # Tab 5: Raw Data
    with tab5:
        st.header("Raw Data Explorer")
        
        if hitting_filtered.empty and pitching_filtered.empty and standings_filtered.empty:
            st.info("Please select years from the sidebar to view raw data")
        else:
            data_type = st.selectbox("Select data type", ["Hitting Leaders", "Pitching Leaders", "Team Standings"])
            
            if data_type == "Hitting Leaders" and not hitting_filtered.empty:
                st.dataframe(hitting_filtered, hide_index=True)
            elif data_type == "Pitching Leaders" and not pitching_filtered.empty:
                st.dataframe(pitching_filtered, hide_index=True)
            elif data_type == "Team Standings" and not standings_filtered.empty:
                st.dataframe(standings_filtered, hide_index=True)
            else:
                st.warning(f"No data available for {data_type}")
    
    # Footer
    st.markdown("---")
    st.markdown("**Data Pipeline:** Web Scraping → Data Quality → SQLite Database → Interactive Dashboard")

if __name__ == "__main__":
    main()