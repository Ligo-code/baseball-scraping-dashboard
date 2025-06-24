import streamlit as st
import sqlite3
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import os

# Page configuration
st.set_page_config(
    page_title="MLB Historical Data Dashboard",
    page_icon="⚾",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for better styling
st.markdown("""
<style>
    .main-header {
        font-size: 3rem;
        font-weight: bold;
        text-align: center;
        color: #1f4e79;
        margin-bottom: 2rem;
    }
    .metric-card {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 0.5rem;
        border-left: 4px solid #1f4e79;
    }
    .sidebar .sidebar-content {
        background-color: #f8f9fa;
    }
</style>
""", unsafe_allow_html=True)

@st.cache_data
def load_data():
    """Load all data from database with caching"""
    db_path = 'data/mlb_database.db'
    if not os.path.exists(db_path):
        st.error(f"Database not found at {db_path}. Please run the import script first.")
        return None, None, None, None
    
    try:
        conn = sqlite3.connect(db_path)
        standings = pd.read_sql_query("SELECT * FROM standings", conn)
        hitting = pd.read_sql_query("SELECT * FROM hitting_leaders", conn)
        pitching = pd.read_sql_query("SELECT * FROM pitching_leaders", conn)
        events = pd.read_sql_query("SELECT * FROM notable_events", conn)
        conn.close()
        
        return standings, hitting, pitching, events
    except Exception as e:
        st.error(f"Error loading data: {e}")
        return None, None, None, None

def create_wins_distribution_chart(standings_df):
    """Create wins distribution visualization"""
    fig = px.histogram(
        standings_df, 
        x='wins', 
        title='Distribution of Team Wins Across All Years',
        labels={'wins': 'Wins', 'count': 'Number of Teams'},
        color_discrete_sequence=['#2E86C1']  # Better blue color
    )
    fig.update_layout(
        xaxis_title="Wins",
        yaxis_title="Number of Teams",
        showlegend=False,
        plot_bgcolor='white'
    )
    return fig

def create_team_performance_timeline(standings_df, selected_teams):
    """Create team performance over time"""
    if not selected_teams:
        return go.Figure()
    
    filtered_df = standings_df[standings_df['team_name'].isin(selected_teams)]
    
    fig = px.line(
        filtered_df,
        x='year',
        y='wins',
        color='team_name',
        title='Team Performance Over Time',
        labels={'wins': 'Wins', 'year': 'Year', 'team_name': 'Team'}
    )
    
    fig.update_layout(
        xaxis_title="Year",
        yaxis_title="Wins",
        legend_title="Team"
    )
    
    return fig

def create_home_run_leaders_chart(hitting_df):
    """Create home run leaders visualization"""
    hr_leaders = hitting_df[hitting_df['stat_category'] == 'Home Runs'].copy()
    hr_leaders = hr_leaders.sort_values('stat_value', ascending=True)
    
    # Create a discrete color map for each player/year combination
    fig = go.Figure()
    
    # Define distinct colors for better visibility
    colors = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd', 
              '#8c564b', '#e377c2', '#7f7f7f', '#bcbd22', '#17becf']
    
    for i, (_, row) in enumerate(hr_leaders.iterrows()):
        fig.add_trace(go.Bar(
            x=[row['stat_value']],
            y=[f"{row['player_name']} ({row['year']})"],
            orientation='h',
            name=f"{row['year']}",
            marker_color=colors[i % len(colors)],
            text=[row['stat_value']],
            textposition='outside',
            showlegend=False
        ))
    
    fig.update_layout(
        title='Home Run Leaders by Year',
        xaxis_title="Home Runs",
        yaxis_title="Player (Year)",
        height=600,
        plot_bgcolor='white'
    )
    
    return fig

def create_era_vs_wins_scatter(pitching_df, standings_df):
    """Create ERA vs team wins scatter plot"""
    era_leaders = pitching_df[pitching_df['stat_category'] == 'ERA'].copy()
    
    # Try to match teams using substring matching
    merged_data = []
    for _, pitcher in era_leaders.iterrows():
        year = pitcher['year']
        team_abbrev = pitcher['team']
        
        # Find matching team in standings
        year_standings = standings_df[standings_df['year'] == year]
        
        # Try different matching strategies
        matched_team = None
        for _, team in year_standings.iterrows():
            team_name = team['team_name']
            # Check if team abbreviation matches part of team name
            if (team_abbrev in team_name or 
                team_abbrev.lower() in team_name.lower() or
                any(word in team_name for word in team_abbrev.split())):
                matched_team = team
                break
        
        if matched_team is not None:
            merged_data.append({
                'player_name': pitcher['player_name'],
                'team': pitcher['team'],
                'era': pitcher['stat_value'],
                'year': pitcher['year'],
                'team_wins': matched_team['wins'],
                'team_name': matched_team['team_name']
            })
    
    if not merged_data:
        return go.Figure()
    
    merged_df = pd.DataFrame(merged_data)
    
    fig = px.scatter(
        merged_df,
        x='era',
        y='team_wins',
        color='year',
        size='team_wins',
        hover_data=['player_name', 'team_name'],
        title='ERA Leaders vs Team Performance',
        labels={'era': 'ERA', 'team_wins': 'Team Wins'}
    )
    
    fig.update_layout(
        xaxis_title="ERA (lower is better)",
        yaxis_title="Team Wins"
    )
    
    return fig

def create_events_timeline(events_df):
    """Create timeline of notable events"""
    event_counts = events_df.groupby(['year', 'event_type']).size().reset_index(name='count')
    
    fig = px.bar(
        event_counts,
        x='year',
        y='count',
        color='event_type',
        title='Notable Events Timeline by Type',
        labels={'count': 'Number of Events', 'year': 'Year', 'event_type': 'Event Type'}
    )
    
    fig.update_layout(
        xaxis_title="Year",
        yaxis_title="Number of Events",
        legend_title="Event Type"
    )
    
    return fig

def create_batting_average_comparison(hitting_df):
    """Create batting average comparison across years"""
    ba_leaders = hitting_df[hitting_df['stat_category'] == 'Batting Average'].copy()
    
    fig = px.box(
        ba_leaders,
        x='year',
        y='stat_value',
        title='Batting Average Leaders Distribution by Year',
        labels={'stat_value': 'Batting Average', 'year': 'Year'}
    )
    
    fig.update_layout(
        xaxis_title="Year",
        yaxis_title="Batting Average"
    )
    
    return fig

def main():
    # Main header
    st.markdown('<h1 class="main-header">⚾ MLB Historical Data Dashboard</h1>', unsafe_allow_html=True)
    
    # Load data
    standings_df, hitting_df, pitching_df, events_df = load_data()
    
    if standings_df is None:
        st.stop()
    
    # Sidebar controls
    st.sidebar.header("Dashboard Controls")
    st.sidebar.markdown("---")
    
    # Year selection
    available_years = sorted(standings_df['year'].unique())
    selected_years = st.sidebar.multiselect(
        "Select Years to Analyze",
        options=available_years,
        default=available_years,
        help="Choose which years to include in the analysis"
    )
    
    # Team selection for performance tracking
    available_teams = sorted(standings_df['team_name'].unique())
    selected_teams = st.sidebar.multiselect(
        "Select Teams for Timeline",
        options=available_teams,
        default=['New York Yankees', 'Boston Red Sox'],
        help="Choose teams to display in the performance timeline"
    )
    
    # Filter data based on selections
    filtered_standings = standings_df[standings_df['year'].isin(selected_years)]
    filtered_hitting = hitting_df[hitting_df['year'].isin(selected_years)]
    filtered_pitching = pitching_df[pitching_df['year'].isin(selected_years)]
    filtered_events = events_df[events_df['year'].isin(selected_years)]
    
    # Key metrics
    st.header("📊 Key Statistics")
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            label="Years Covered", 
            value=len(selected_years),
            help="Number of years in the selected dataset"
        )
    
    with col2:
        st.metric(
            label="Teams Analyzed", 
            value=len(filtered_standings),
            help="Total team seasons in selected years"
        )
    
    with col3:
        max_wins = filtered_standings['wins'].max() if not filtered_standings.empty else 0
        best_team = filtered_standings.loc[filtered_standings['wins'].idxmax(), 'team_name'] if not filtered_standings.empty else "N/A"
        st.metric(
            label="Best Season", 
            value=f"{max_wins} wins",
            help=f"Best record: {best_team}"
        )
    
    with col4:
        total_events = len(filtered_events)
        st.metric(
            label="Notable Events", 
            value=total_events,
            help="Total notable events recorded"
        )
    
    st.markdown("---")
    
    # Main visualizations
    st.header("📈 Team Performance Analysis")
    
    # Row 1: Team performance and wins distribution
    col1, col2 = st.columns(2)
    
    with col1:
        if selected_teams:
            timeline_fig = create_team_performance_timeline(filtered_standings, selected_teams)
            st.plotly_chart(timeline_fig, use_container_width=True)
        else:
            st.info("Select teams in the sidebar to view performance timeline")
    
    with col2:
        wins_fig = create_wins_distribution_chart(filtered_standings)
        st.plotly_chart(wins_fig, use_container_width=True)
    
    st.markdown("---")
    
    # Row 2: Player statistics
    st.header("🏆 Player Performance")
    
    col1, col2 = st.columns(2)
    
    with col1:
        if not filtered_hitting[filtered_hitting['stat_category'] == 'Home Runs'].empty:
            hr_fig = create_home_run_leaders_chart(filtered_hitting)
            st.plotly_chart(hr_fig, use_container_width=True)
        else:
            st.info("No home run data available for selected years")
    
    with col2:
        if not filtered_hitting[filtered_hitting['stat_category'] == 'Batting Average'].empty:
            ba_fig = create_batting_average_comparison(filtered_hitting)
            st.plotly_chart(ba_fig, use_container_width=True)
        else:
            st.info("No batting average data available for selected years")
    
    st.markdown("---")
    
    # Row 3: Events and pitching analysis
    st.header("📅 Historical Events & Pitching")
    
    col1, col2 = st.columns(2)
    
    with col1:
        if not filtered_events.empty:
            events_fig = create_events_timeline(filtered_events)
            st.plotly_chart(events_fig, use_container_width=True)
        else:
            st.info("No events data available for selected years")
    
    with col2:
        if not filtered_pitching[filtered_pitching['stat_category'] == 'ERA'].empty:
            era_fig = create_era_vs_wins_scatter(filtered_pitching, filtered_standings)
            if era_fig.data:  # Check if figure has data
                st.plotly_chart(era_fig, use_container_width=True)
            else:
                st.info("Unable to match ERA data with team records")
        else:
            st.info("No ERA data available for selected years")
    
    st.markdown("---")
    
    # Data tables section
    st.header("📋 Detailed Data Tables")
    
    tab1, tab2, tab3, tab4 = st.tabs(["🏟️ Standings", "🏏 Hitting Leaders", "⚾ Pitching Leaders", "📰 Notable Events"])
    
    with tab1:
        st.subheader("Team Standings")
        display_standings = filtered_standings.sort_values(['year', 'wins'], ascending=[True, False])
        st.dataframe(display_standings, use_container_width=True)
    
    with tab2:
        st.subheader("Hitting Statistical Leaders")
        if not filtered_hitting.empty:
            # Add category filter
            hitting_categories = sorted(filtered_hitting['stat_category'].unique())
            selected_hitting_cat = st.selectbox("Select Hitting Category", hitting_categories)
            
            hitting_display = filtered_hitting[filtered_hitting['stat_category'] == selected_hitting_cat]
            hitting_display = hitting_display.sort_values(['year', 'stat_value'], ascending=[True, False])
            st.dataframe(hitting_display, use_container_width=True)
        else:
            st.info("No hitting data available for selected years")
    
    with tab3:
        st.subheader("Pitching Statistical Leaders")
        if not filtered_pitching.empty:
            # Add category filter
            pitching_categories = sorted(filtered_pitching['stat_category'].unique())
            selected_pitching_cat = st.selectbox("Select Pitching Category", pitching_categories)
            
            pitching_display = filtered_pitching[filtered_pitching['stat_category'] == selected_pitching_cat]
            
            # Sort differently for ERA (lower is better)
            if selected_pitching_cat == 'ERA':
                pitching_display = pitching_display.sort_values(['year', 'stat_value'], ascending=[True, True])
            else:
                pitching_display = pitching_display.sort_values(['year', 'stat_value'], ascending=[True, False])
            
            st.dataframe(pitching_display, use_container_width=True)
        else:
            st.info("No pitching data available for selected years")
    
    with tab4:
        st.subheader("Notable Events")
        if not filtered_events.empty:
            # Add event type filter
            event_types = ['All'] + sorted(filtered_events['event_type'].unique())
            selected_event_type = st.selectbox("Select Event Type", event_types)
            
            if selected_event_type == 'All':
                events_display = filtered_events
            else:
                events_display = filtered_events[filtered_events['event_type'] == selected_event_type]
            
            events_display = events_display.sort_values('year', ascending=False)
            st.dataframe(events_display, use_container_width=True)
        else:
            st.info("No events data available for selected years")
    
    # Footer
    st.markdown("---")
    st.markdown("""
    ### About This Dashboard
    This interactive dashboard displays historical MLB data from significant years in baseball history. 
    The data includes team standings, individual player statistical leaders, and notable events.
    
    **Data Sources**: Web scraped from Baseball Almanac historical records  
    **Years Covered**: 1927, 1947, 1961, 1969, 1994, 1998, 2001, 2016, 2020, 2023  
    **Last Updated**: 2025
    
    Use the sidebar controls to filter data and explore different aspects of baseball history!
    """)

if __name__ == "__main__":
    main()