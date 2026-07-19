import streamlit as st

st.set_page_config(
    page_title="Near-Earth Objects",
    page_icon="☄️",
    layout="wide"
)

from utils.queries import (get_total_neos_today,get_hazardous_neos_today,
                           get_total_neos_recorded,get_total_hazardous_recorded,
                           get_daily_neo_trend,get_hazardous_trend,get_top_5_largest_neos,
                           get_top_5_fastest_neos,get_top_5_closest_neos)

import plotly.express as px
import pandas as pd

if st.button("← Back to Home"):
    st.switch_page("app.py")

st.title("☄️ Near-Earth Objects Dashboard")

st.write("Welcome to the Near-Earth Objects analytics Dashboard.")

st.divider()
st.subheader("Key Metrics")
col1,col2,col3,col4 = st.columns(4)

with col1:
    df = get_total_neos_today()
    st.metric("Total Neos Today",df.iloc[0,0])
with col2:
    df = get_hazardous_neos_today()
    st.metric("Hazardous NEOs Today",df.iloc[0,0])
with col3:
    df=get_total_neos_recorded()
    st.metric("Total NEOs Recorded",df.iloc[0,0])
with col4:
    df=get_total_hazardous_recorded()
    st.metric("Total Hazardous NEOs Recorded",df.iloc[0,0])

st.markdown("")
st.divider()
st.subheader("Daily Trends")
df_daily_neo = get_daily_neo_trend()
df_daily_neo['summary_date'] = pd.to_datetime(df_daily_neo['summary_date'])
df_daily_neo=df_daily_neo.sort_values("summary_date")

daily_neo_fig = px.line(
    df_daily_neo,
    x="summary_date",
    y="total_neo_count",
    markers = True,
    title="Daily NEO Trend"
)

st.plotly_chart(daily_neo_fig,use_container_width=True)

df_hazard_daily = get_hazardous_trend()
df_hazard_daily['summary_date'] = pd.to_datetime(df_hazard_daily['summary_date'])
df_hazard_daily=df_hazard_daily.sort_values("summary_date")

daily_hazard_trend = px.line(
    df_hazard_daily,
    x="summary_date",
    y="potentially_hazardous_count",
    markers = True,
    title = "Daily Hazardous NEO Trend"
)

st.plotly_chart(daily_hazard_trend,use_container_width=True)


st.markdown("")
st.divider()
st.subheader("Top Asteroid Rankings")

filter_col_1,filter_col_2 = st.columns([4,1])

with filter_col_2:
    asteroid_type = st.selectbox("Asteroid Type",["All","Hazardous","Non-Hazardous"],index=0)


col5,col6,col7 = st.columns(3)

with col5:
    df = get_top_5_largest_neos()
    if asteroid_type != 'All':
        df = df[df['asteroid_type']==asteroid_type]
    df = df.sort_values(by="estimated_diameter_max_kms",ascending=True)
    fig = px.bar(
        df,
        y="asteroid_name",
        x="estimated_diameter_max_kms",
        title="Top 5 Largest NEOs"
    )
    fig.update_layout(
        yaxis_title="",
        xaxis_title="Diameter (km)",
        showlegend=False
    )
    st.plotly_chart(fig,use_container_width=True)
with col6:
    df = get_top_5_fastest_neos()
    if asteroid_type != 'All':
        df = df[df['asteroid_type']==asteroid_type]
    df = df.sort_values(by="relative_velocity_kmph",ascending=True)
    
    fig = px.bar(
        df,
        y="asteroid_name",
        x="relative_velocity_kmph",
        title="Top 5 Fastest NEOs"
    )
    fig.update_layout(
        yaxis_title="",
        xaxis_title="Velocity (kmph)",
        showlegend=False
    )
    st.plotly_chart(fig,use_container_width=True)
with col7:
    df = get_top_5_closest_neos()
    if asteroid_type != 'All':
        df = df[df['asteroid_type']==asteroid_type]
    df = df.sort_values(by="miss_distance_kms",ascending=False)
    
    fig = px.bar(
        df,
        y="asteroid_name",
        x="miss_distance_kms",
        title="Top 5 Closest NEOs"
    )
    fig.update_layout(
        yaxis_title="",
        xaxis_title="Miss Distance (kms)",
        showlegend=False
    )
    st.plotly_chart(fig,use_container_width=True)