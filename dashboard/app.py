import streamlit as st
from streamlit_folium import st_folium
import folium
import pandas as pd
from google.cloud import bigquery
from datetime import datetime, timedelta
import plotly.express as px
import plotly.graph_objects as go

# Set up BigQuery client
@st.cache_resource
def get_bigquery_client():
    return bigquery.Client()

# Function to query BigQuery data
@st.cache_data(ttl=3600)
def query_city_data(city, time_range='7d'):
    client = get_bigquery_client()
    
    # Calculate the date range
    end_date = datetime.now()
    if time_range == '7d':
        start_date = end_date - timedelta(days=7)
    elif time_range == '30d':
        start_date = end_date - timedelta(days=30)
    elif time_range == '90d':
        start_date = end_date - timedelta(days=90)
    
    start_date_str = start_date.strftime('%Y-%m-%d')
    
    if city == "New York":
        # Query for NYC data
        emergency_query = f"""
            SELECT creation_date, incident_type, borough, latitude, longitude
            FROM `city_insights.raw_nyc_emergency_responses`
            LIMIT 1000
        """
        
        collision_query = f"""
            SELECT crash_date, borough, number_of_persons_injured, number_of_persons_killed, latitude, longitude
            FROM `city_insights.raw_nyc_vehicle_collisions_crashes`
            LIMIT 1000
        """
        
        arrests_query = f"""
            SELECT arrest_date, arrest_boro, arrest_type, latitude, longitude
            FROM `city_insights.raw_nyc_arrest_data`
            LIMIT 1000
        """
        
        emergency_df = client.query(emergency_query).to_dataframe()
        collision_df = client.query(collision_query).to_dataframe()
        arrests_df = client.query(arrests_query).to_dataframe()
        
        return {
            "emergency": emergency_df,
            "collisions": collision_df,
            "arrests": arrests_df
        }
        
    # Add queries for other cities when data becomes available
    else:
        # Return empty dataframes for cities without data
        return {
            "emergency": pd.DataFrame(),
            "collisions": pd.DataFrame(),
            "arrests": pd.DataFrame()
        }

# Function to create visualizations
def create_visualizations(city_data, city):
    visualizations = {}
    
    if city == "New York":
        # Process emergency data
        if not city_data["emergency"].empty:
            # Incident type distribution
            incident_counts = city_data["emergency"]["incident_type"].value_counts().reset_index()
            incident_counts.columns = ["Incident Type", "Count"]
            
            fig_incidents = px.bar(
                incident_counts.head(10), 
                x="Incident Type", 
                y="Count",
                title="Top 10 Emergency Incident Types"
            )
            visualizations["incident_types"] = fig_incidents
            
            # Borough distribution
            borough_counts = city_data["emergency"]["borough"].value_counts().reset_index()
            borough_counts.columns = ["Borough", "Count"]
            
            fig_boroughs = px.pie(
                borough_counts, 
                names="Borough", 
                values="Count",
                title="Emergency Incidents by Borough"
            )
            visualizations["emergency_boroughs"] = fig_boroughs
            
        # Process collision data
        if not city_data["collisions"].empty:
            # Injuries over time
            collision_df = city_data["collisions"].copy()
            collision_df["date"] = pd.to_datetime(collision_df["crash_date"]).dt.date
            daily_injuries = collision_df.groupby("date").agg({
                "number_of_persons_injured": "sum",
                "number_of_persons_killed": "sum"
            }).reset_index()
            
            fig_injuries = px.line(
                daily_injuries, 
                x="date", 
                y=["number_of_persons_injured", "number_of_persons_killed"],
                title="Daily Injuries and Fatalities from Vehicle Collisions",
                labels={"value": "Count", "variable": "Type"}
            )
            visualizations["collision_trends"] = fig_injuries
            
        # Process arrest data
        if not city_data["arrests"].empty:
            # Arrest types
            arrest_counts = city_data["arrests"]["arrest_type"].value_counts().reset_index()
            arrest_counts.columns = ["Arrest Type", "Count"]
            
            fig_arrests = px.bar(
                arrest_counts.head(10), 
                x="Arrest Type", 
                y="Count",
                title="Top 10 Arrest Types"
            )
            visualizations["arrest_types"] = fig_arrests
    
    # Add visualizations for other cities when data becomes available
    
    return visualizations

# Main Streamlit app
st.set_page_config(layout="wide", page_title="City Insights Dashboard")

# Sidebar for filters
with st.sidebar:
    st.title("City Insights Dashboard")
    st.markdown("Select a city on the map to view insights")
    
    time_range = st.selectbox(
        "Time Range",
        options=["7 days", "30 days", "90 days"],
        index=0
    )
    
    time_dict = {
        "7 days": "7d",
        "30 days": "30d",
        "90 days": "90d"
    }
    
    selected_range = time_dict[time_range]

# Create and display the map
st.header("City Selection Map")
m = folium.Map(location=[20, 0], zoom_start=3)

# Add pins for each supported city
city_data = {
    (40.7128, -74.0060): "New York",
    (34.0522, -118.2437): "Los Angeles",
    (48.8566, 2.3522): "Paris",
    (-23.5505, -46.6333): "São Paulo"
}

for coords, city in city_data.items():
    folium.Marker(coords, tooltip=city).add_to(m)

# Display map in Streamlit
st_data = st_folium(m, height=300, use_container_width=True)

# Handle click
if st_data['last_object_clicked']:
    # Extract the coordinates from the clicked object
    lat = st_data['last_object_clicked']['lat']
    lng = st_data['last_object_clicked']['lng']
    
    # Find the closest city based on coordinates
    closest_city = None
    min_distance = float('inf')
    
    for coords, city in city_data.items():
        city_lat, city_lng = coords
        distance = ((city_lat - lat)**2 + (city_lng - lng)**2)**0.5
        if distance < min_distance:
            min_distance = distance
            closest_city = city
    
    if closest_city:
        st.header(f"Data for {closest_city}")
        
        # Add loading indicator
        with st.spinner(f"Loading data for {closest_city}..."):
            # Query BigQuery for city data
            city_data_results = query_city_data(closest_city, selected_range)
            
            # Create tabs for different data categories
            tab1, tab2, tab3 = st.tabs(["Emergency Data", "Vehicle Collisions", "Arrest Data"])
            
            with tab1:
                if not city_data_results["emergency"].empty:
                    st.subheader("Emergency Response Data")
                    
                    # Create visualizations
                    visualizations = create_visualizations(city_data_results, closest_city)
                    
                    # Display visualizations in columns
                    if "incident_types" in visualizations:
                        st.plotly_chart(visualizations["incident_types"], use_container_width=True)
                    
                    if "emergency_boroughs" in visualizations:
                        st.plotly_chart(visualizations["emergency_boroughs"], use_container_width=True)
                    
                    # Show raw data in expandable section
                    with st.expander("View Raw Emergency Data"):
                        st.dataframe(city_data_results["emergency"].head(100))
                else:
                    st.info(f"No emergency data available for {closest_city}")
            
            with tab2:
                if not city_data_results["collisions"].empty:
                    st.subheader("Vehicle Collision Data")
                    
                    # Display collision trend visualization
                    if "collision_trends" in visualizations:
                        st.plotly_chart(visualizations["collision_trends"], use_container_width=True)
                    
                    # Show summary statistics
                    total_injuries = city_data_results["collisions"]["number_of_persons_injured"].sum()
                    total_fatalities = city_data_results["collisions"]["number_of_persons_killed"].sum()
                    
                    col1, col2 = st.columns(2)
                    with col1:
                        st.metric("Total Injuries", total_injuries)
                    with col2:
                        st.metric("Total Fatalities", total_fatalities)
                    
                    # Show raw data in expandable section
                    with st.expander("View Raw Collision Data"):
                        st.dataframe(city_data_results["collisions"].head(100))
                else:
                    st.info(f"No collision data available for {closest_city}")
            
            with tab3:
                if not city_data_results["arrests"].empty:
                    st.subheader("Arrest Data")
                    
                    # Display arrest visualization
                    if "arrest_types" in visualizations:
                        st.plotly_chart(visualizations["arrest_types"], use_container_width=True)
                    
                    # Show raw data in expandable section
                    with st.expander("View Raw Arrest Data"):
                        st.dataframe(city_data_results["arrests"].head(100))
                else:
                    st.info(f"No arrest data available for {closest_city}")