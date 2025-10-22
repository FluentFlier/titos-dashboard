import streamlit as st
import boto3
import pandas as pd
import time
import os

# -----------------------------
# AWS + Athena Setup
# -----------------------------
ATHENA_DB = "titos_cap_monitoring"
ATHENA_TABLE = "parsed_logs"
S3_OUTPUT = "s3://cisek-inspections-ml-data/athena-results/"
REGION = "us-west-2"

session = boto3.Session(region_name=REGION)
athena = session.client('athena')

def run_athena_query(query):
    response = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={'Database': ATHENA_DB},
        ResultConfiguration={'OutputLocation': S3_OUTPUT},
    )
    execution_id = response['QueryExecutionId']
    
    # Wait for the query to complete
    while True:
        status = athena.get_query_execution(QueryExecutionId=execution_id)
        state = status['QueryExecution']['Status']['State']
        if state in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
            break
        time.sleep(1)

    if state == 'SUCCEEDED':
        result = athena.get_query_results(QueryExecutionId=execution_id)
        return execution_id
    else:
        st.error(f"Athena query failed with status: {state}")
        return None

def fetch_athena_result_as_df(execution_id):
    result = athena.get_query_results(QueryExecutionId=execution_id)
    rows = result['ResultSet']['Rows']
    headers = [col['VarCharValue'] for col in rows[0]['Data']]
    data = [[col.get('VarCharValue', '') for col in row['Data']] for row in rows[1:]]
    return pd.DataFrame(data, columns=headers)

# -----------------------------
# App UI
# -----------------------------
st.title("🧪 Titos Cap Inspection Dashboard")
st.markdown("Monitoring defect events from automated inspections")

# -----------------------------
# Inspection-Level Data
# -----------------------------
st.subheader("📋 Inspection Table (Raw Events)")
inspection_query = f"""
SELECT timestamp, bottle_id, camera_id, defect_type, confidence
FROM {ATHENA_TABLE}
ORDER BY timestamp DESC
LIMIT 100
"""
exec_id = run_athena_query(inspection_query)
if exec_id:
    inspection_df = fetch_athena_result_as_df(exec_id)
    st.dataframe(inspection_df)

# -----------------------------
# Bottle-Level Aggregation
# -----------------------------
st.subheader("🧴 Bottle Summary")
bottle_query = f"""
SELECT
    bottle_id,
    COUNT(*) AS total_inspections,
    COUNT_IF(defect_type IS NOT NULL AND defect_type != '') AS total_defects
FROM {ATHENA_TABLE}
GROUP BY bottle_id
ORDER BY total_inspections DESC
LIMIT 100
"""
exec_id2 = run_athena_query(bottle_query)
if exec_id2:
    bottle_df = fetch_athena_result_as_df(exec_id2)
    st.dataframe(bottle_df)

# -----------------------------
# Graphs
# -----------------------------
st.subheader("📈 Defects Over Time")
defect_time_query = f"""
SELECT
    date_trunc('minute', from_iso8601_timestamp(timestamp)) AS minute,
    COUNT_IF(defect_type IS NOT NULL AND defect_type != '') AS defect_count
FROM {ATHENA_TABLE}
GROUP BY minute
ORDER BY minute
LIMIT 1000
"""
exec_id3 = run_athena_query(defect_time_query)
if exec_id3:
    defect_df = fetch_athena_result_as_df(exec_id3)
    defect_df['minute'] = pd.to_datetime(defect_df['minute'])
    defect_df['defect_count'] = defect_df['defect_count'].astype(int)
    st.line_chart(defect_df.set_index('minute'))

# -----------------------------
# Footer
# -----------------------------
