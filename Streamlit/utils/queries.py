from utils.Config import get_engine
import pandas as pd


engine = get_engine()

def get_total_neos_today():
    query = """
    select total_neo_count from 
    astrosight.gold.neo_summary t where t.summary_date = current_date
    """
    return pd.read_sql(query,engine)

def get_hazardous_neos_today():
    query = """
    select t.potentially_hazardous_count from 
    astrosight.gold.neo_summary t where t.summary_date = current_date
    """
    return pd.read_sql(query,engine)

def get_total_neos_recorded():
    query = """
    select sum(total_neo_count) as total_count from astrosight.gold.neo_summary
    """
    return pd.read_sql(query,engine)

def get_total_hazardous_recorded():
    query = """
    select sum(potentially_hazardous_count) as total_hazardous_count from astrosight.gold.neo_summary
    """
    return pd.read_sql(query,engine)

def get_daily_neo_trend():
    query = """
    select summary_date,total_neo_count from astrosight.gold.neo_summary order by summary_date
    """
    return pd.read_sql(query,engine)

def get_hazardous_trend():
    query = """
    select summary_date,potentially_hazardous_count from astrosight.gold.neo_summary order by summary_date
    """
    return pd.read_sql(query,engine)

def get_top_5_largest_neos():
    query = """
    select asteroid_name,estimated_diameter_max_kms,largest_rank, 
    case when t.is_potentially_hazardous = True then 'Hazardous' else 'Non-Hazardous' end as asteroid_type
    from astrosight.gold.neo_rankings t where t.largest_rank <=5 order by largest_rank asc
    """
    return pd.read_sql(query,engine)

def get_top_5_fastest_neos():
    query = """
    select asteroid_name,relative_velocity_kmph,fastest_rank, 
    case when t.is_potentially_hazardous = True then 'Hazardous' else 'Non-Hazardous' end as asteroid_type
    from astrosight.gold.neo_rankings t where t.fastest_rank <=5 order by fastest_rank asc
    """
    return pd.read_sql(query,engine)

def get_top_5_closest_neos():
    query = """
    select asteroid_name,miss_distance_kms,closest_rank,close_approach_date_full,
    case when t.is_potentially_hazardous = True then 'Hazardous' else 'Non-Hazardous' end as asteroid_type
    from astrosight.gold.neo_rankings t where t.closest_rank <=5 order by closest_rank asc
    """
    return pd.read_sql(query,engine)

def get_last_refresh():
    query = """
    select max(pipeline_end_time) as last_refresh from astrosight.bronze.pipeline_audit t 
    where t.pipeline_run_date=current_date and t.pipeline_stage_status='PASSED'
    """
    return pd.read_sql(query,engine)