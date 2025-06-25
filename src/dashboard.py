import streamlit as st
import sqlite3
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import numpy as np
import os

# Page configuration
st.set_page_config(
    page_title="MLB Historical Analysis: The Evolution of Baseball",
    page_icon="⚾",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for better styling
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        text-align: center;
        color: #1f4e79;
        margin-bottom: 1rem;
    }
    .sub-header {
        font-size: 1.2rem;
        text-align: center;
        color: #666;
        margin-bottom: 2rem;
        font-style: italic;
    }
    .metric-card {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 0.5rem;
        border-left: 4px solid #1f4e79;
    }
    .era-context {
        background-color: #e8f4f8;
        padding: 1rem;
        border-radius: 0.5rem;
        margin: 1rem 0;
    }
</style>
""", unsafe_allow_html=True)

@st.cache_data
def load_data():
    """Load all data from database or CSV files with caching"""
    
    # Try to load from database first
    db_path = 'data/mlb_database.db'
    if os.path.exists(db_path):
        try:
            conn = sqlite3.connect(db_path)
            standings = pd.read_sql_query("SELECT * FROM standings", conn)
            hitting = pd.read_sql_query("SELECT * FROM hitting_leaders", conn)
            pitching = pd.read_sql_query("SELECT * FROM pitching_leaders", conn)
            events = pd.read_sql_query("SELECT * FROM notable_events", conn)
            conn.close()
            return standings, hitting, pitching, events
        except Exception as e:
            st.warning(f"Could not load from database: {e}. Trying CSV files...")
    
    # Fallback to cleaned CSV files
    cleaned_dir = 'data/cleaned'
    raw_dir = 'data/raw'
    
    # Try cleaned data first, then raw data
    for data_dir in [cleaned_dir, raw_dir]:
        try:
            standings = pd.read_csv(f'{data_dir}/team_standings{"_cleaned" if data_dir == cleaned_dir else ""}.csv')
            hitting = pd.read_csv(f'{data_dir}/yearly_hitting_leaders{"_cleaned" if data_dir == cleaned_dir else ""}.csv')
            pitching = pd.read_csv(f'{data_dir}/yearly_pitching_leaders{"_cleaned" if data_dir == cleaned_dir else ""}.csv')
            events = pd.read_csv(f'{data_dir}/notable_events{"_cleaned" if data_dir == cleaned_dir else ""}.csv')
            
            st.info(f"Loaded data from {data_dir}/ directory")
            return standings, hitting, pitching, events
            
        except FileNotFoundError:
            continue
    
    # If no data found
    st.error("No data files found. Please run the scraper first: `python src/scraper.py`")
    return None, None, None, None

def get_era_context(year):
    """Get historical context for each era"""
    era_info = {
        1927: {"era": "Murderers' Row", "context": "Babe Ruth's 60 HR season, Yankees dominance, Live Ball Era peak"},
        1947: {"era": "Integration Era", "context": "Jackie Robinson breaks color barrier, post-WWII baseball resurgence"},
        1961: {"era": "Expansion Era", "context": "Maris breaks Ruth's record, AL expands to 10 teams, 162-game season begins"},
        1969: {"era": "End of Pitcher Era", "context": "Mound lowered, strike zone reduced, divisional play begins"},
        1994: {"era": "Strike Season", "context": "Season ended by strike, beginning of offensive explosion"},
        1998: {"era": "Home Run Chase", "context": "McGwire vs Sosa, offensive numbers reach historic highs"},
        2001: {"era": "Bonds' Peak", "context": "Barry Bonds 73 HRs, post-9/11 season, steroid era continues"},
        2016: {"era": "Analytics Era", "context": "Cubs break 108-year drought, advanced metrics reshape game"},
        2020: {"era": "COVID Season", "context": "60-game season, DH in both leagues, empty stadiums"},
        2023: {"era": "Modern Rules", "context": "Pitch clock, shift restrictions, larger bases implemented"}
    }
    return era_info.get(year, {"era": "Unknown", "context": "No context available"})

def create_home_run_evolution(hitting_df):
    """Create home run evolution across eras with context"""
    hr_data = hitting_df[hitting_df['stat_category'] == 'Home Runs'].copy()
    hr_data = hr_data.sort_values('year')
    
    # Add era context
    hr_data['era_info'] = hr_data['year'].apply(lambda x: get_era_context(x)['era'])
    hr_data['context'] = hr_data['year'].apply(lambda x: get_era_context(x)['context'])
    
    fig = go.Figure()
    
    # Color mapping for eras
    era_colors = {
        "Murderers' Row": "#1f77b4",
        "Integration Era": "#ff7f0e", 
        "Expansion Era": "#2ca02c",
        "End of Pitcher Era": "#d62728",
        "Strike Season": "#9467bd",
        "Home Run Chase": "#8c564b",
        "Bonds' Peak": "#e377c2",
        "Analytics Era": "#7f7f7f",
        "COVID Season": "#bcbd22",
        "Modern Rules": "#17becf"
    }
    
    for _, row in hr_data.iterrows():
        fig.add_trace(go.Scatter(
            x=[row['year']],
            y=[row['stat_value']],
            mode='markers+text',
            name=row['era_info'],
            marker=dict(
                size=20,
                color=era_colors.get(row['era_info'], '#999999'),
                line=dict(width=2, color='white')
            ),
            text=f"{row['player_name']}<br>{int(row['stat_value'])} HRs",
            textposition="top center",
            hovertemplate=f"<b>{row['player_name']}</b><br>" +
                         f"Year: {row['year']}<br>" +
                         f"Home Runs: {int(row['stat_value'])}<br>" +
                         f"Era: {row['era_info']}<br>" +
                         f"Context: {row['context']}<extra></extra>",
            showlegend=False
        ))
    
    fig.update_layout(
        title='Evolution of Home Run Records Across Baseball Eras',
        xaxis_title="Year",
        yaxis_title="Home Runs",
        height=600,
        plot_bgcolor='white',
        hovermode='closest'
    )
    
    # Add trend line
    fig.add_trace(go.Scatter(
        x=hr_data['year'],
        y=hr_data['stat_value'],
        mode='lines',
        name='Trend',
        line=dict(color='rgba(128,128,128,0.5)', width=2, dash='dash'),
        hoverinfo='skip'
    ))
    
    return fig

def create_team_dominance_analysis(standings_df, hitting_df, pitching_df):
    """Analyze what factors contribute to team dominance"""
    
    # Get top teams (>100 wins or top win% for each year)
    dominant_teams = []
    for year in standings_df['year'].unique():
        year_data = standings_df[standings_df['year'] == year]
        top_team = year_data.loc[year_data['wins'].idxmax()]
        dominant_teams.append(top_team)
    
    dominant_df = pd.DataFrame(dominant_teams)
    
    # Better team matching function
    def match_team_names(full_team_name, player_team_name):
        """Better team name matching between full names and city names"""
        if pd.isna(player_team_name):
            return False
        
        full_name_lower = str(full_team_name).lower()
        player_team_lower = str(player_team_name).lower()
        
        # Direct match
        if player_team_lower in full_name_lower:
            return True
        
        # Handle common variations
        team_mappings = {
            'new york yankees': ['new york', 'yankees'],
            'boston red sox': ['boston', 'red sox'],
            'detroit tigers': ['detroit', 'tigers'],
            'chicago white sox': ['chicago', 'white sox'],
            'cleveland indians': ['cleveland', 'indians'],
            'cleveland guardians': ['cleveland', 'guardians'],
            'baltimore orioles': ['baltimore', 'orioles'],
            'minnesota twins': ['minnesota', 'twins'],
            'oakland athletics': ['oakland', 'athletics'],
            'kansas city royals': ['kansas city', 'royals'],
            'seattle mariners': ['seattle', 'mariners'],
            'texas rangers': ['texas', 'rangers'],
            'houston astros': ['houston', 'astros'],
            'los angeles angels': ['los angeles', 'anaheim', 'california', 'angels'],
            'toronto blue jays': ['toronto', 'blue jays'],
            'tampa bay rays': ['tampa bay', 'rays']
        }
        
        for full_name, variations in team_mappings.items():
            if full_name in full_name_lower:
                return any(var in player_team_lower for var in variations)
        
        return False
    
    # Try to match with offensive/pitching performance
    analysis_data = []
    for _, team in dominant_df.iterrows():
        year = team['year']
        team_name = team['team_name']
        
        # Count statistical leaders from this team
        year_hitting = hitting_df[hitting_df['year'] == year]
        year_pitching = pitching_df[pitching_df['year'] == year]
        
        # Better matching using the function above
        hitting_leaders = len(year_hitting[
            year_hitting['team'].apply(lambda x: match_team_names(team_name, x))
        ])
        
        pitching_leaders = len(year_pitching[
            year_pitching['team'].apply(lambda x: match_team_names(team_name, x))
        ])
        
        analysis_data.append({
            'year': year,
            'team': team_name,
            'wins': team['wins'],
            'win_pct': team['win_pct'],
            'hitting_leaders': hitting_leaders,
            'pitching_leaders': pitching_leaders,
            'total_leaders': hitting_leaders + pitching_leaders,
            'era': get_era_context(year)['era']
        })
    
    analysis_df = pd.DataFrame(analysis_data)
    
    # Create visualization
    fig = go.Figure()
    
    # Bubble chart: wins vs total leaders, sized by win percentage
    fig.add_trace(go.Scatter(
        x=analysis_df['total_leaders'],
        y=analysis_df['wins'],
        mode='markers+text',
        marker=dict(
            size=analysis_df['win_pct'] * 150,  # Scale for visibility
            color=analysis_df['year'],
            colorscale='Viridis',
            showscale=True,
            colorbar=dict(title="Year"),
            line=dict(width=2, color='white'),
            opacity=0.7
        ),
        text=analysis_df.apply(lambda row: f"{row['team'].split()[-1]}<br>{row['year']}", axis=1),
        textposition="middle center",
        textfont=dict(size=10, color='white'),
        hovertemplate="<b>%{text}</b><br>" +
                     "Wins: %{y}<br>" +
                     "Statistical Leaders: %{x}<br>" +
                     "Win Pct: %{marker.size:.1f}%<br>" +
                     "Hitting Leaders: %{customdata[0]}<br>" +
                     "Pitching Leaders: %{customdata[1]}<extra></extra>",
        customdata=analysis_df[['hitting_leaders', 'pitching_leaders']],
        name="Dominant Teams"
    ))
    
    fig.update_layout(
        title='Team Success vs Statistical Leaders: What Drives Dominance?',
        xaxis_title="Number of Statistical Category Leaders",
        yaxis_title="Team Wins",
        height=600,
        plot_bgcolor='white'
    )
    
    return fig, analysis_df

def create_offensive_evolution_comparison(hitting_df):
    """Compare key offensive categories across eras"""
    
    # More meaningful offensive categories to track
    key_stats = ['Home Runs', 'Batting Average', 'RBI', 'On Base Percentage']
    
    fig = make_subplots(
        rows=2, cols=2,
        subplot_titles=[
            'Home Runs (Power)',
            'Batting Average (Contact)', 
            'RBI (Run Production)',
            'On Base Percentage (Getting on Base)'
        ],
        specs=[[{"secondary_y": False}, {"secondary_y": False}],
               [{"secondary_y": False}, {"secondary_y": False}]]
    )
    
    colors = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728']
    
    for i, stat in enumerate(key_stats):
        row = (i // 2) + 1
        col = (i % 2) + 1
        
        stat_data = hitting_df[hitting_df['stat_category'] == stat].copy()
        stat_data = stat_data.sort_values('year')
        
        if not stat_data.empty:
            fig.add_trace(
                go.Scatter(
                    x=stat_data['year'],
                    y=stat_data['stat_value'],
                    mode='lines+markers',
                    name=stat,
                    line=dict(color=colors[i], width=3),
                    marker=dict(size=10, line=dict(width=2, color='white')),
                    hovertemplate=f"<b>{stat}</b><br>" +
                                 "Year: %{x}<br>" +
                                 "Leader: %{y}<br>" +
                                 "Player: %{customdata}<br>" +
                                 "<extra></extra>",
                    customdata=stat_data['player_name']
                ),
                row=row, col=col
            )
    
    # Add annotations for key insights
    fig.add_annotation(
        text="Peak of Steroid Era",
        x=1998, y=70,  # Approximate position for home runs
        showarrow=True,
        arrowhead=2,
        row=1, col=1
    )
    
    fig.update_layout(
        title='Evolution of Key Offensive Statistics: Power vs Contact vs Patience',
        height=700,
        showlegend=False
    )
    
    # Update y-axis titles
    fig.update_yaxes(title_text="Home Runs", row=1, col=1)
    fig.update_yaxes(title_text="Batting Average", row=1, col=2)
    fig.update_yaxes(title_text="RBI", row=2, col=1)
    fig.update_yaxes(title_text="On Base %", row=2, col=2)
    
    return fig

def create_historical_events_timeline(events_df):
    """Create an improved timeline of historical events"""
    
    events_df = events_df.copy()
    
    # Count events by year and category
    event_counts = events_df.groupby(['year', 'event_type']).size().reset_index(name='count')
    
    # Create timeline with better colors
    color_map = {
        'Championships': '#FFD700',        # Gold
        'Records Broken': '#FF6B6B',       # Red
        'Pitching Feats': '#4ECDC4',       # Teal
        'Player Debuts': '#45B7D1',        # Blue
        'Career Endings': '#96CEB4',       # Light Green
        'Deaths': '#574B90',               # Purple
        'Awards & Honors': '#FFA07A',      # Orange
        'Trades & Signings': '#DDA0DD',    # Plum
        'Labor Issues': '#F4A460',         # Sandy Brown
        'Rule Changes': '#20B2AA',         # Light Sea Green
        'Stadium Events': '#87CEEB',       # Sky Blue
        'Injuries': '#CD5C5C',             # Indian Red
        'Ceremonies': '#DAA520',           # Goldenrod
        'Season Events': '#9370DB',        # Medium Purple
        'Home Run Events': '#FF69B4',      # Hot Pink
        'Team Milestones': '#32CD32',      # Lime Green
        'Game Highlights': '#FF7F50',      # Coral
        'Historical Notes': '#708090'       # Slate Gray
    }
    
    fig = px.bar(
        event_counts,
        x='year',
        y='count',
        color='event_type',
        title='Historical Baseball Events: Tracking the Evolution of America\'s Pastime',
        labels={'count': 'Number of Events', 'year': 'Year', 'event_type': 'Event Category'},
        color_discrete_map=color_map
    )
    
    # Add annotations for key eras
    annotations = []
    era_years = [1927, 1947, 1961, 1969, 1994, 1998, 2001, 2016, 2020, 2023]
    for year in era_years:
        era_info = get_era_context(year)
        year_total = event_counts[event_counts['year'] == year]['count'].sum()
        if year_total > 0:
            annotations.append(
                dict(
                    x=year,
                    y=year_total + 0.5,
                    text=era_info['era'],
                    showarrow=True,
                    arrowhead=2,
                    arrowsize=1,
                    arrowwidth=1,
                    arrowcolor="black",
                    font=dict(size=9),
                    bgcolor="rgba(255,255,255,0.8)",
                    bordercolor="black",
                    borderwidth=1
                )
            )
    
    fig.update_layout(
        annotations=annotations,
        height=600,
        xaxis_title="Year",
        yaxis_title="Number of Events",
        legend_title="Event Category",
        legend=dict(
            orientation="v",
            yanchor="top",
            y=1,
            xanchor="left",
            x=1.02
        )
    )
    
    return fig

def main():
    # Main header with project purpose
    st.markdown('<h1 class="main-header">⚾ MLB Historical Analysis Dashboard</h1>', unsafe_allow_html=True)
    st.markdown('<div class="sub-header">Analyzing the Evolution of Baseball Performance Across Pivotal Eras</div>', unsafe_allow_html=True)
    
    # Project purpose explanation
    st.markdown("""
    <div class="era-context">
    <h3>🎯 Research Objective</h3>
    This dashboard explores how baseball has evolved across different eras by analyzing statistical leaders, team performance, 
    and historical events from 10 pivotal years in MLB history. We examine the relationship between individual excellence 
    and team success, and how external factors (rule changes, social changes, technology) have shaped the game.
    </div>
    """, unsafe_allow_html=True)
    
    # Load data
    standings_df, hitting_df, pitching_df, events_df = load_data()
    
    if standings_df is None:
        st.stop()
    
    # Sidebar controls
    st.sidebar.header("🔧 Analysis Controls")
    st.sidebar.markdown("---")
    
    # Era selection with context
    st.sidebar.subheader("📅 Select Historical Eras")
    available_years = sorted(standings_df['year'].unique())
    
    # Show era information
    era_options = {}
    for year in available_years:
        era_info = get_era_context(year)
        era_options[f"{year} - {era_info['era']}"] = year
    
    selected_era_labels = st.sidebar.multiselect(
        "Choose Eras to Analyze",
        options=list(era_options.keys()),
        default=list(era_options.keys()),
        help="Select which historical eras to include in your analysis"
    )
    
    selected_years = [era_options[label] for label in selected_era_labels]
    
    # Filter data
    filtered_standings = standings_df[standings_df['year'].isin(selected_years)]
    filtered_hitting = hitting_df[hitting_df['year'].isin(selected_years)]
    filtered_pitching = pitching_df[pitching_df['year'].isin(selected_years)]
    filtered_events = events_df[events_df['year'].isin(selected_years)]
    
    # Key insights section
    st.header("📊 Key Insights")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        total_eras = len(selected_years)
        st.metric("Eras Analyzed", total_eras, help="Number of historical periods in analysis")
    
    with col2:
        if not filtered_hitting.empty:
            max_hrs = filtered_hitting[filtered_hitting['stat_category'] == 'Home Runs']['stat_value'].max()
            hr_leader = filtered_hitting[
                (filtered_hitting['stat_category'] == 'Home Runs') & 
                (filtered_hitting['stat_value'] == max_hrs)
            ]['player_name'].iloc[0]
            st.metric("HR Record", f"{int(max_hrs)}", help=f"Highest single-season HR total: {hr_leader}")
        else:
            st.metric("HR Record", "N/A")
    
    with col3:
        if not filtered_standings.empty:
            best_record = filtered_standings['wins'].max()
            best_team = filtered_standings[filtered_standings['wins'] == best_record]['team_name'].iloc[0]
            best_year = filtered_standings[filtered_standings['wins'] == best_record]['year'].iloc[0]
            st.metric("Best Record", f"{int(best_record)} wins", help=f"{best_team} ({best_year})")
        else:
            st.metric("Best Record", "N/A")
    
    with col4:
        total_events = len(filtered_events)
        st.metric("Historical Events", total_events, help="Total documented events across selected eras")
    
    st.markdown("---")
    
    # Main Analysis Section
    st.header("🔍 Historical Analysis")
    
    # Tab layout for different analyses
    tab1, tab2, tab3, tab4 = st.tabs([
        "🏆 Offensive Evolution", 
        "📈 Team Dominance", 
        "📅 Historical Timeline", 
        "📋 Detailed Data"
    ])
    
    with tab1:
        st.subheader("How Offensive Capabilities Have Evolved")
        
        col1, col2 = st.columns([2, 1])
        
        with col1:
            if not filtered_hitting.empty:
                hr_evolution_fig = create_home_run_evolution(filtered_hitting)
                st.plotly_chart(hr_evolution_fig, use_container_width=True)
            else:
                st.info("No offensive data available for selected eras")
        
        with col2:
            st.markdown("### 📈 Key Trends & Statistics Explained")
            
            # Explanation of key statistics
            st.markdown("""
            **Key Offensive Statistics:**
            - **Home Runs**: Ultimate power statistic, shows raw offensive capability
            - **Batting Average**: Contact ability (hits ÷ at-bats), classic measure
            - **RBI**: Run production, measures clutch hitting ability  
            - **On Base Percentage**: Modern statistic showing plate discipline
            
            **Why These Matter:**
            - Power (HR) drives modern offense strategy
            - Contact (BA) shows consistent hitting ability
            - Production (RBI) measures situational hitting
            - Patience (OBP) correlates strongly with team success
            """)
            
            if not filtered_hitting[filtered_hitting['stat_category'] == 'Home Runs'].empty:
                hr_data = filtered_hitting[filtered_hitting['stat_category'] == 'Home Runs']
                hr_trend = hr_data.groupby('year')['stat_value'].max()
                
                st.markdown("**Home Run Evolution:**")
                for year in selected_years:
                    if year in hr_trend.index:
                        era_info = get_era_context(year)
                        player_name = hr_data[
                            (hr_data['year'] == year) & 
                            (hr_data['stat_value'] == hr_trend[year])
                        ]['player_name'].iloc[0]
                        st.write(f"• **{year}** ({era_info['era']}): {int(hr_trend[year])} HRs - {player_name}")
                
                # Calculate era progression
                if len(hr_trend) > 1:
                    early_avg = hr_trend[hr_trend.index <= 1961].mean()
                    modern_avg = hr_trend[hr_trend.index >= 1994].mean()
                    if not pd.isna(early_avg) and not pd.isna(modern_avg):
                        change = ((modern_avg - early_avg) / early_avg) * 100
                        st.metric("Power Evolution", f"+{change:.1f}%", 
                                 help=f"Increase from early era ({early_avg:.1f}) to modern era ({modern_avg:.1f})")
                        
                        # Add context about stolen bases
                        st.markdown("""
                        **📊 Why We Focus on These Stats:**
                        Unlike stolen bases (which peaked in the 1980s but became less strategic), 
                        these four statistics represent the core of offensive value across all eras. 
                        Home runs drive modern strategy, while OBP correlates most strongly with winning.
                        """)
            else:
                st.info("Select years with home run data to see evolution trends")
        
        # Offensive categories comparison
        if not filtered_hitting.empty:
            st.subheader("Comparing Key Offensive Categories")
            offensive_comparison = create_offensive_evolution_comparison(filtered_hitting)
            st.plotly_chart(offensive_comparison, use_container_width=True)
    
    with tab2:
        st.subheader("What Drives Team Success?")
        
        if not filtered_standings.empty:
            dominance_fig, dominance_data = create_team_dominance_analysis(
                filtered_standings, filtered_hitting, filtered_pitching
            )
            st.plotly_chart(dominance_fig, use_container_width=True)
            
            # Analysis insights
            col1, col2 = st.columns(2)
            
            with col1:
                st.markdown("### 🏆 Championship Teams Analysis")
                avg_leaders = dominance_data['total_leaders'].mean()
                top_teams = dominance_data.nlargest(3, 'wins')
                
                st.write(f"**Average Statistical Leaders per Dominant Team:** {avg_leaders:.1f}")
                st.write("**Most Dominant Teams:**")
                for _, team in top_teams.iterrows():
                    st.write(f"• {team['team']} ({team['year']}): {team['wins']} wins, {team['total_leaders']} category leaders")
            
            with col2:
                st.markdown("### 📊 Success Correlation")
                if len(dominance_data) > 3:
                    correlation = dominance_data['wins'].corr(dominance_data['total_leaders'])
                    st.metric("Wins-Leaders Correlation", f"{correlation:.3f}", 
                             help="Correlation between team wins and statistical category leaders")
                
                # Era with most dominant teams
                era_dominance = dominance_data.groupby('era')['wins'].mean().sort_values(ascending=False)
                if not era_dominance.empty:
                    top_era = era_dominance.index[0]
                    st.write(f"**Most Dominant Era:** {top_era}")
                    st.write(f"**Average Wins:** {era_dominance.iloc[0]:.1f}")
        else:
            st.info("No team data available for selected eras")
    
    with tab3:
        st.subheader("Historical Events and Context")
        
        # Add explanation about the data
        st.info("""
        📖 **About Historical Events**: These events were extracted from baseball almanac pages using web scraping. 
        Each event represents a significant moment in baseball history, automatically classified by type based on content analysis.
        Click on any year below to see the full descriptions of events.
        """)
        
        if not filtered_events.empty:
            timeline_fig = create_historical_events_timeline(filtered_events)
            st.plotly_chart(timeline_fig, use_container_width=True)
            
            # Show era contexts
            st.subheader("📖 Era Context")
            for year in selected_years:
                era_info = get_era_context(year)
                year_events = filtered_events[filtered_events['year'] == year]
                
                with st.expander(f"{year} - {era_info['era']} ({len(year_events)} events)"):
                    st.write(f"**Historical Context:** {era_info['context']}")
                    
                    if not year_events.empty:
                        st.write("**Key Events:**")
                        event_types = year_events['event_type'].value_counts()
                        for event_type, count in event_types.head(5).items():
                            st.write(f"• {event_type}: {count} events")
                        
                        # Show detailed events in a more readable format
                        st.write("**Sample Events:**")
                        sample_events = year_events.sample(min(3, len(year_events)))
                        for _, event in sample_events.iterrows():
                            # Create a cleaner display of events
                            event_text = event['description']
                            
                            # Truncate very long descriptions but keep them meaningful
                            if len(event_text) > 200:
                                # Try to break at sentence end
                                sentences = event_text.split('.')
                                truncated = sentences[0]
                                if len(truncated) < 150 and len(sentences) > 1:
                                    truncated += '. ' + sentences[1]
                                event_text = truncated + '...'
                            
                            # Display with better formatting
                            st.markdown(f"""
                            **{event['event_type']}**: {event_text}
                            """)
                            
                        # Add option to see all events
                        if len(year_events) > 3:
                            if st.button(f"Show all {len(year_events)} events for {year}", key=f"show_all_{year}"):
                                st.write("**All Events:**")
                                for _, event in year_events.iterrows():
                                    with st.expander(f"{event['event_type']}: {event['description'][:80]}..."):
                                        st.write(f"**Category:** {event['event_type']}")
                                        st.write(f"**Full Description:** {event['description']}")
        else:
            st.info("No events data available for selected eras")
    
    with tab4:
        st.subheader("Detailed Statistical Data")
        
        # Sub-tabs for different data types
        data_tab1, data_tab2, data_tab3, data_tab4 = st.tabs([
            "🏟️ Team Records", "🏏 Hitting Leaders", "⚾ Pitching Leaders", "📰 Historical Events"
        ])
        
        with data_tab1:
            if not filtered_standings.empty:
                st.dataframe(
                    filtered_standings.sort_values(['year', 'wins'], ascending=[True, False]),
                    use_container_width=True,
                    hide_index=True
                )
            else:
                st.info("No standings data available")
        
        with data_tab2:
            if not filtered_hitting.empty:
                # Focus on most important hitting categories
                important_hitting_categories = [
                    'All', 'Home Runs', 'Batting Average', 'RBI', 'Runs', 
                    'On Base Percentage', 'Slugging Average', 'Hits', 'Doubles'
                ]
                
                available_categories = ['All'] + sorted([cat for cat in filtered_hitting['stat_category'].unique() 
                                                       if cat in important_hitting_categories[1:]])
                
                selected_hitting_cat = st.selectbox(
                    "Select Hitting Category", 
                    available_categories, 
                    key="hitting_cat",
                    help="Showing most strategically important offensive statistics"
                )
                
                if selected_hitting_cat == 'All':
                    # Show only important categories when "All" is selected
                    display_hitting = filtered_hitting[
                        filtered_hitting['stat_category'].isin(important_hitting_categories[1:])
                    ]
                else:
                    display_hitting = filtered_hitting[filtered_hitting['stat_category'] == selected_hitting_cat]
                
                display_hitting = display_hitting.sort_values(['year', 'stat_value'], ascending=[True, False])
                
                # Add explanation
                if selected_hitting_cat != 'All':
                    stat_explanations = {
                        'Home Runs': "Ultimate power statistic - drives modern offensive strategy",
                        'Batting Average': "Classic contact statistic (hits ÷ at-bats)",
                        'RBI': "Run production - measures clutch situational hitting",
                        'On Base Percentage': "Modern statistic showing plate discipline and getting on base",
                        'Slugging Average': "Power metric (total bases ÷ at-bats)",
                        'Runs': "Scoring ability - correlates with offensive contribution"
                    }
                    
                    if selected_hitting_cat in stat_explanations:
                        st.info(f"📊 **{selected_hitting_cat}**: {stat_explanations[selected_hitting_cat]}")
                
                st.dataframe(display_hitting, use_container_width=True, hide_index=True)
            else:
                st.info("No hitting data available")
        
        with data_tab3:
            if not filtered_pitching.empty:
                pitch_categories = ['All'] + sorted(filtered_pitching['stat_category'].unique())
                selected_pitching_cat = st.selectbox("Select Pitching Category", pitch_categories, key="pitching_cat")
                
                if selected_pitching_cat == 'All':
                    display_pitching = filtered_pitching
                else:
                    display_pitching = filtered_pitching[filtered_pitching['stat_category'] == selected_pitching_cat]
                
                # Sort ERA differently (lower is better)
                if selected_pitching_cat == 'ERA':
                    display_pitching = display_pitching.sort_values(['year', 'stat_value'], ascending=[True, True])
                else:
                    display_pitching = display_pitching.sort_values(['year', 'stat_value'], ascending=[True, False])
                
                st.dataframe(display_pitching, use_container_width=True, hide_index=True)
            else:
                st.info("No pitching data available")
        
        with data_tab4:
            if not filtered_events.empty:
                event_types = ['All'] + sorted(filtered_events['event_type'].unique())
                selected_event_type = st.selectbox("Select Event Type", event_types, key="event_type")
                
                if selected_event_type == 'All':
                    display_events = filtered_events
                else:
                    display_events = filtered_events[filtered_events['event_type'] == selected_event_type]
                
                display_events = display_events.sort_values('year', ascending=False)
                
                # Show events in a more readable format
                for _, event in display_events.iterrows():
                    with st.expander(f"{event['year']} - {event['event_type']}: {event['description'][:60]}..."):
                        st.write(f"**Year:** {event['year']}")
                        st.write(f"**Category:** {event['event_type']}")
                        st.write(f"**Description:** {event['description']}")
            else:
                st.info("No events data available")
    
    # Methodology section
    st.markdown("---")
    st.header("🔬 Methodology & Data Sources")
    
    with st.expander("📊 Data Collection & Processing"):
        st.markdown("""
        ### Web Scraping Process
        - **Source**: Baseball-Almanac.com historical records
        - **Tools**: Selenium WebDriver for dynamic content, BeautifulSoup for parsing
        - **Challenges Addressed**:
          - User-agent rotation to prevent blocking
          - Fallback mechanisms for failed requests
          - Duplicate detection and removal
          - Text cleaning and normalization
        
        ### Data Cleaning & Validation
        - **Player Names**: Standardized formatting and duplicate removal
        - **Team Names**: Cross-era mapping and normalization
        - **Statistics**: Range validation and outlier detection
        - **Events**: Keyword-based classification and content filtering
        
        ### Quality Assurance
        - Validated win totals (40-130 range for reasonable seasons)
        - Cross-referenced major records with known historical facts
        - Removed non-baseball content and navigation artifacts
        """)
    
    with st.expander("🎯 Research Questions & Findings"):
        st.markdown("""
        ### Primary Research Questions
        1. **How have offensive capabilities evolved?**
           - Home run totals show clear upward trend from 1927 to 2001
           - Notable spikes during expansion eras and steroid period
           - Modern era shows optimization rather than raw power increases
        
        2. **What factors correlate with team success?**
           - Strong correlation between offensive leaders and team wins
           - Pitching dominance more important in earlier eras
           - Modern teams require balanced excellence across categories
        
        3. **How do historical events impact performance?**
           - Rule changes (mound height, strike zone) dramatically affect statistics
           - Social changes (integration) expanded talent pool and competition
           - Technology and analytics have optimized but not revolutionized performance
        """)
    
    # Footer with technical details
    st.markdown("---")
    st.markdown("""
    ### 📚 About This Analysis
    
    **Data Coverage**: 10 pivotal years spanning 1927-2023, representing major eras in baseball history
    
    **Technical Stack**: 
    - **Scraping**: Selenium, BeautifulSoup, Requests
    - **Processing**: Pandas, SQLite
    - **Visualization**: Streamlit, Plotly
    - **Analysis**: Statistical correlation, trend analysis
    
    **Limitations**: 
    - Team name matching across eras requires manual verification
    - Event classification based on text analysis may miss nuanced categories
    - Statistical categories vary slightly across different historical periods
    
    **Future Enhancements**: 
    - Expand to include more years and advanced metrics
    - Add predictive modeling based on historical trends
    - Include geographic analysis of talent distribution
    
    ---
    *Built for Python Analytics course- June 2025*
    """)

if __name__ == "__main__":
    main()