import streamlit as st




# ----------------------------------------------------
# Page Configuration
# ----------------------------------------------------
st.set_page_config(
    page_title="AstroSight",
    page_icon="🌌",
    layout="wide"
)

# ----------------------------------------------------
# Hero Section
# ----------------------------------------------------
st.title("🌌 AstroSight")

st.subheader("End-to-End NASA Data Analytics Platform")

from utils.queries import get_last_refresh




st.divider()

st.write(
    """
    AstroSight is an interactive analytics platform that enables users to
    explore NASA's public datasets through intuitive dashboards,
    transforming complex space data into meaningful insights.
    """
)

# st.write(
#     """
#     Explore NASA's public datasets through interactive dashboards designed 
#     to uncover trends, visualize space data, and transform complex information 
#     into meaningful insights.
#     """
# )
st.divider()
last_refresh = get_last_refresh().iloc[0]['last_refresh'].strftime("%d %b %Y, %I:%M %p UTC")

st.info(f"🕓Last Data Refresh:{last_refresh}")
# ----------------------------------------------------
# Available Dashboards
# ----------------------------------------------------
st.header("🚀 Available Dashboards")

st.caption("Choose a dashboard to explore NASA's public datasets.")

# ----------------------------------------------------
# Near-Earth Objects
# ----------------------------------------------------
st.subheader("☄️ Near-Earth Objects")

st.write(
    "Analyze asteroid trends, hazardous objects, close approaches, "
    "and other asteroid-related analytics."
)

if st.button("Open Dashboard →", key="neo"):
    st.switch_page("pages/NEO_page.py")

st.divider()

# ----------------------------------------------------
# Astronomy Picture of the Day
# ----------------------------------------------------
st.subheader("🌌 Astronomy Picture of the Day")

st.write(
    "Browse NASA's daily astronomy images and explore "
    "historical APOD collections."
)

st.button("Open Dashboard →", key="apod")

st.divider()

# ----------------------------------------------------
# Coming Soon
# ----------------------------------------------------
st.subheader("🌍 Earth Events")

st.caption("Coming Soon")

st.divider()

st.subheader("🚀 Mars Rover")

st.caption("Coming Soon")


