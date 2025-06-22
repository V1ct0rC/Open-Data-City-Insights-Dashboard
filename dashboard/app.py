import streamlit as st
from streamlit_folium import st_folium
import folium


st.set_page_config(layout="wide", page_title="City Insights Dashboard")
m = folium.Map(location=[20, 0], zoom_start=3)

# Add pins for each supported city
city_data = {
    (40.7128, -74.0060): "New York",
    (48.8566, 2.3522): "Paris",
    (-23.5505, -46.6333): "São Paulo"
}

for coords, city in city_data.items():
    folium.Marker(coords, tooltip=city).add_to(m)

# Display map in Streamlit
st_data = st_folium(m, height=800, use_container_width=True)

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
        st.subheader(f"Data for {closest_city}")
        # Load and display data for the selected city
    
    print(f"Clicked on {closest_city} at ({lat}, {lng})")