from sqlalchemy import create_engine
import streamlit as st


@st.cache_resource
def get_engine():
    engine = create_engine("trino://trino@astrosight-trino:8080")
    return engine