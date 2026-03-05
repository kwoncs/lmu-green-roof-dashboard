import streamlit as st
import pandas as pd
import plotly.express as px
from plotly.subplots import make_subplots
import plotly.graph_objects as go
from api_clients import fetch_lmu_weather_data, fetch_purpleair_history

# --- 1. Presentation Configuration ---
# Wide layout and collapsed sidebar prioritize the data on a projector screen
st.set_page_config(
    page_title="LMU Green Roof Analysis", 
    layout="wide", 
    initial_sidebar_state="collapsed"
)

# Custom CSS for high-contrast metric cards and clean typography
st.markdown("""
    <style>
    div[data-testid="metric-container"] {
        background-color: #1E1E1E;
        border-radius: 8px;
        padding: 15px;
        border: 1px solid #333;
        box-shadow: 2px 2px 10px rgba(0,0,0,0.5);
    }
    .stTabs [data-baseweb="tab-list"] {
        gap: 24px;
    }
    .stTabs [data-baseweb="tab"] {
        height: 50px;
        white-space: pre-wrap;
        font-size: 1.2rem;
    }
    </style>
""", unsafe_allow_html=True)

st.title("LMU Green Roof: Baseline parameters")
st.markdown("Data acquired from Open-Meteo (ECMWF) and Purple Air APIs")

# --- 2. Data Pipeline ---
API_KEY = "7B8D10F8-1755-11F1-B596-4201AC1DC123"
SENSOR_ID = 34481
START_DATE = "2026-01-01"
END_DATE = "2026-01-31" # Expanded range for better 3D point density

@st.cache_data(ttl=3600)
def load_and_merge_data():
    om_df = fetch_lmu_weather_data(START_DATE, END_DATE)
    pa_df = fetch_purpleair_history(API_KEY, SENSOR_ID, START_DATE, END_DATE)
    
    if om_df is None or pa_df is None or om_df.empty or pa_df.empty:
        return pd.DataFrame()
        
    om_df = om_df.sort_index().reset_index()
    pa_df = pa_df.sort_index().reset_index()
    
    merged_df = pd.merge_asof(
        om_df, pa_df, on='time', direction='nearest', tolerance=pd.Timedelta('1 hour')
    )
    merged_df.set_index('time', inplace=True)
    return merged_df

with st.spinner("Aggregating environmental datasets..."):
    df = load_and_merge_data()

if df.empty:
    st.error("Data aggregation failed. Verify network connection and API limits.")
    st.stop()

# --- 3. Conference Presentation Layout (Updated for Soil Moisture Focus) ---
tab1, tab2, tab3 = st.tabs(["Meterological Parameters", "3D Visualization", "Sensor Data and Export"])

with tab1:
    st.subheader("High-Level Data Insights")
    col1, col2, col3, col4 = st.columns(4)
    # Replaced Max PM2.5 with Max Soil Moisture
    col1.metric("Max Soil Moisture", f"{df['soil_moisture_0_to_7cm'].max():.3f}")
    col2.metric("Avg Temperature 2m (°C)", f"{df['temperature_2m'].mean():.1f}")
    col3.metric("Total Precipitation (mm)", f"{df['precipitation'].sum():.1f}")
    col4.metric("Mean Soil Moisture", f"{df['soil_moisture_0_to_7cm'].mean():.3f}")

    st.divider()
    
    # Dual-Axis Chart: Temperature vs. Soil Moisture
    fig_dual = make_subplots(specs=[[{"secondary_y": True}]])
    # Baseline Temperature
    fig_dual.add_trace(go.Scatter(x=df.index, y=df['temperature_2m'], name="Baseline Temp (°F)", line=dict(color="#FF5733")), secondary_y=False)
    # Soil Moisture (Bar)
    fig_dual.add_trace(go.Bar(x=df.index, y=df['soil_moisture_0_to_7cm'], name="Soil Moisture (m³/m³)", marker_color="#00CC96", opacity=0.5), secondary_y=True)
    
    fig_dual.update_layout(title_text="Timeline: Regional Temperature vs. Native Soil Moisture", hovermode="x unified")
    fig_dual.update_yaxes(title_text="Temperature (°F)", secondary_y=False)
    fig_dual.update_yaxes(title_text="Volumetric Water Content", secondary_y=True)
    st.plotly_chart(fig_dual, use_container_width=True)

with tab2:
    st.subheader("Subsurface Variable Interactions")
    st.markdown("This 3D space visualizes the relationship between regional temperature, humidity, and native soil moisture baselines.")
    
    # Clean NaNs specifically for 3D plotting
    plot_df = df.dropna(subset=['temperature_2m', 'humidity', 'soil_moisture_0_to_7cm', 'pm2.5_atm'])
    
    # 3D Interactive Scatter Plot: SWAPPED Z-AXIS TO SOIL MOISTURE
    fig_3d = px.scatter_3d(
        plot_df,
        x='temperature_2m',
        y='humidity',
        z='soil_moisture_0_to_7cm', # Swapped from pm2.5_atm
        color='soil_moisture_0_to_7cm', # Color gradient tracks moisture depth
        size_max=10,
        opacity=0.8,
        color_continuous_scale=px.colors.sequential.deep, # 'Deep' blue scale fits moisture theme
        labels={
            'temperature_2m': 'Temp (°F)',
            'humidity': 'Humidity (%)',
            'soil_moisture_0_to_7cm': 'Soil Moisture',
            'pm2.5_atm': 'PM2.5'
        },
        title="3D Soil-Climate Correlation Matrix"
    )
    
    fig_3d.update_layout(
        scene=dict(
            xaxis=dict(backgroundcolor="black", gridcolor="gray"),
            yaxis=dict(backgroundcolor="black", gridcolor="gray"),
            zaxis=dict(backgroundcolor="black", gridcolor="gray"),
            camera=dict(eye=dict(x=1.5, y=1.5, z=1.2))
        ),
        margin=dict(l=0, r=0, b=0, t=40)
    )
    st.plotly_chart(fig_3d, use_container_width=True, height=700)
    
with tab3:
    st.subheader("Merged Baseline Data")
    st.dataframe(df, use_container_width=True)
    
    csv_data = df.to_csv().encode('utf-8')
    st.download_button(
        label="Download Clean Dataset (CSV)",
        data=csv_data,
        file_name="lmu_california_meadow_baseline.csv",
        mime="text/csv",
    )