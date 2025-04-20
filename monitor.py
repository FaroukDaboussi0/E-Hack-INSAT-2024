
import streamlit as st
import websocket
import json
import pandas as pd
import altair as alt
import threading
import time
import queue
import requests
from datetime import datetime


data_queue = queue.Queue()


st.set_page_config(page_title="Gas Sensor Dashboard", layout="wide")
st.title("Real-time Gas Sensor Dashboard")


url_last_minute = 'http://localhost:8080/last-minute'
url_last_30_minutes = 'http://localhost:8080/last-30-minutes'


if 'data' not in st.session_state:
    st.session_state.data = pd.DataFrame(columns=['sensor_id', 'checkpoint', 'gas', 'value', 'timestamp', 'datetime'])

if 'connection_status' not in st.session_state:
    st.session_state.connection_status = "Initializing..."


status_placeholder = st.empty()
chart_placeholder = st.empty()


def update_status():
    if st.session_state.connection_status == "Connected":
        status_placeholder.success("WebSocket connection established")
    elif st.session_state.connection_status.startswith("Error"):
        status_placeholder.error(st.session_state.connection_status)
    elif st.session_state.connection_status == "Closed":
        status_placeholder.warning("WebSocket connection closed")
    else:
        status_placeholder.info(st.session_state.connection_status)


update_status()
def fetch_historical_data(url):
    try:
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            
            df_list = []
            skipped_count = 0 
            for item in data:
                dt = datetime.fromisoformat(item['timestamp'])
                item['datetime'] = dt
                df_list.append(item)
            
            if df_list:
                return pd.DataFrame(df_list)
            else:
                return pd.DataFrame(columns=['sensor_id', 'checkpoint', 'gas', 'value', 'timestamp', 'datetime'])
            
           
        else:
            st.error(f"API Error: {response.status_code}")
            return pd.DataFrame(columns=['sensor_id', 'checkpoint', 'gas', 'value', 'timestamp', 'datetime'])
    except Exception as e:
        st.error(f"Fetch failed: {str(e)}")
        return pd.DataFrame(columns=['sensor_id', 'checkpoint', 'gas', 'value', 'timestamp', 'datetime'])

def on_message(ws, message):
    try:
       
        data = json.loads(message)
        
        dt = datetime.fromisoformat(data['timestamp'])
        
        
        data['datetime'] = dt
        
        
        data_queue.put({"type": "data", "payload": data})
    
    except Exception as e:
        
        print(f"Error processing message: {e}")
        data_queue.put({"type": "error", "payload": str(e)})

def on_error(ws, error):
    print(f"WebSocket error: {error}")
    data_queue.put({"type": "error", "payload": str(error)})

def on_close(ws, close_status_code, close_msg):
    print("WebSocket connection closed")
    data_queue.put({"type": "status", "payload": "Closed"})

def on_open(ws):
    print("WebSocket connection established")
    data_queue.put({"type": "status", "payload": "Connected"})

def websocket_thread_function():
    
    while True:
        try:
            data_queue.put({"type": "status", "payload": "Connecting..."})
            ws_url = "ws://localhost:8080/ws"
            ws = websocket.WebSocketApp(ws_url,
                                        on_message=on_message,
                                        on_error=on_error,
                                        on_close=on_close,
                                        on_open=on_open)
            ws.run_forever()
            
            time.sleep(2)  
        except Exception as e:
            print(f"WebSocket thread error: {e}")
            data_queue.put({"type": "error", "payload": f"Connection error: {str(e)}"})
            time.sleep(2)  


if 'websocket_started' not in st.session_state:
    thread = threading.Thread(target=websocket_thread_function, daemon=True)
    thread.start()
    st.session_state.websocket_started = True
    st.session_state.connection_status = "Connecting..."


def process_queue():
    try:
        
        while True:
            try:
                
                message = data_queue.get(block=True, timeout=0.1)
                
                if message["type"] == "status":
                    st.session_state.connection_status = message["payload"]
                    update_status()
                
                elif message["type"] == "error":
                    st.session_state.connection_status = f"Error: {message['payload']}"
                    update_status()
                
                elif message["type"] == "data":
                    data = message["payload"]
                    
                    if 'gas' not in data:
                        new_data = pd.DataFrame([{
                            'sensor_id': data['sensor_id'],
                            'checkpoint': data['checkpoint_name'],
                            'gas': data['gas_type'],
                            'value': data['value'],
                            'timestamp': data['timestamp'],
                            'datetime': data['id']
                        }])
                    

                    else:

                        new_data = pd.DataFrame([{
                            'sensor_id': data['sensor_id'],
                            'checkpoint': data['checkpoint'],
                            'gas': data['gas'],
                            'value': data['value'],
                            'timestamp': data['timestamp'],
                            'datetime': data['datetime']
                        }])
                    
                    st.session_state.data = pd.concat([st.session_state.data, new_data], ignore_index=True)
                    
                    
                    if len(st.session_state.data) > 100:
                        st.session_state.data = st.session_state.data.iloc[-100:]
                
                
                data_queue.task_done()
                
            except queue.Empty:
                
                break
                
    except Exception as e:
        st.error(f"Error processing queue: {e}")


def create_line_charts(data_df):
    if data_df.empty:
        return None
    
    
    if 'gas' in data_df:
        grouped_data = data_df.groupby(['gas', 'checkpoint'])
    else:
        grouped_data = data_df.groupby(['gas_type', 'checkpoint_name'])
    
    
    charts = []
    for (gas, checkpoint), group_data in grouped_data:
        
        sorted_data = group_data.sort_values('datetime')
        
        title = f"{gas.upper()} - {checkpoint}"
        
        chart = alt.Chart(sorted_data).mark_line().encode(
            x=alt.X('datetime:T', title='Time'),
            y=alt.Y('value:Q', title='Concentration'),
            tooltip=['timestamp:T', 'value:Q']
        ).properties(
            title=title,
            width=800,
            height=300
        )
        
        charts.append(chart)
    
  
    if charts:
        return alt.vconcat(*charts)
    return None

def create_pie_charts(data_df):
    if data_df.empty:
        return None
    
    
    if 'gas_type' in data_df:
        latest_by_checkpoint = data_df.sort_values('datetime').groupby('checkpoint_name').tail(10)
        checkpoint_values = latest_by_checkpoint.groupby(['checkpoint_name', 'gas_type'])['value'].mean().reset_index()
        t = 'checkpoint_name'
        t1= 'gas_type'

    else:
        latest_by_checkpoint = data_df.sort_values('datetime').groupby('checkpoint').tail(10)
        t = 'checkpoint'
        t1= 'gas'
    
    
        checkpoint_values = latest_by_checkpoint.groupby(['checkpoint', 'gas'])['value'].mean().reset_index()
    
    
    pie_charts = []
    
    
    for checkpoint, group_data in checkpoint_values.groupby(t):
        
        present_gases = set(group_data[t1].unique())
        
        
        total_present_value = group_data['value'].sum()
        
        
        if total_present_value == 0:
            continue
        
        
        present_percentage = min(100, total_present_value)
        missing_percentage = max(0, 100 - present_percentage)
        
        
        pie_data = group_data.copy()
        pie_data[t1] = pie_data[t1].str.upper()
        pie_data['percentage'] = pie_data['value'] / total_present_value * present_percentage
        
        
        if missing_percentage > 0:
            other_row = pd.DataFrame({
                'checkpoint': [checkpoint],
                'gas': ['OTHER GASES'],
                'value': [missing_percentage],
                'percentage': [missing_percentage]
            })
            pie_data = pd.concat([pie_data, other_row], ignore_index=True)
        
        
        pie = alt.Chart(pie_data).mark_arc().encode(
            theta=alt.Theta(field="percentage", type="quantitative"),
            color=alt.Color(field=t1, type="nominal"),
            tooltip=[
                alt.Tooltip(t1 + ":N", title="Gas"),
                alt.Tooltip("value:Q", title="Value", format=".4f"),
                alt.Tooltip("percentage:Q", title="Percentage", format=".1f")
            ]
        ).properties(
            title=f"Gas Distribution at {checkpoint}",
            width=400,
            height=400
        )
        
        pie_charts.append(pie)
    
    
    if pie_charts:
        return alt.hconcat(*pie_charts)
    return None


tab1, tab2, tab3 = st.tabs(["Real-time Data", "Last Minute", "Last 30 Minutes"])

with tab1:
    
    rt_line_chart = st.empty()
    rt_pie_chart = st.empty()
    
    
    st.subheader("Latest Sensor Values")
    latest_values = st.empty()
    
    with st.expander("Show Data"):
        raw_data_placeholder = st.empty()

with tab2:
    st.subheader("Data from Last Minute")
    last_min_btn = st.button("Fetch Last Minute Data")
    last_min_line = st.empty()
    last_min_pie = st.empty()
    
    with st.expander("Show Last Minute Data"):
        last_min_raw = st.empty()

with tab3:
    st.subheader("Data from Last 30 Minutes")
    last_30_btn = st.button("Fetch Last 30 Minutes Data")
    last_30_line = st.empty()
    last_30_pie = st.empty()
    

    with st.expander("Show Last 30 Minutes Data"):
        last_30_raw = st.empty()


st.sidebar.header("Dashboard Settings")
refresh_interval = st.sidebar.slider("Refresh interval (seconds)", min_value=1, max_value=30, value=3)
auto_refresh = st.sidebar.checkbox("Enable auto-refresh", value=True)

if st.sidebar.button("Manual Refresh"):
    process_queue()

if last_min_btn:
    with st.spinner("Fetching last minute data..."):
        last_min_data = fetch_historical_data(url_last_minute)
        if not last_min_data.empty:
            last_min_line.altair_chart(create_line_charts(last_min_data), use_container_width=True)
            pie_chart = create_pie_charts(last_min_data)
            if pie_chart:
                last_min_pie.altair_chart(pie_chart, use_container_width=True)
            last_min_raw.dataframe(last_min_data, use_container_width=True)
        else:
            last_min_line.info("No data available for the last minute")

if last_30_btn:
    with st.spinner("Fetching last 30 minutes data..."):
        last_30_data = fetch_historical_data(url_last_30_minutes)
        if not last_30_data.empty:
            last_30_line.altair_chart(create_line_charts(last_30_data), use_container_width=True)
            pie_chart = create_pie_charts(last_30_data)
            if pie_chart:
                last_30_pie.altair_chart(pie_chart, use_container_width=True)
            last_30_raw.dataframe(last_30_data, use_container_width=True)
        else:
            last_30_line.info("No data available for the last 30 minutes")


if auto_refresh:
    while True:
        
        process_queue()
        
        
        line_chart = create_line_charts(st.session_state.data)
        if line_chart:
            rt_line_chart.altair_chart(line_chart, use_container_width=True)
        
        
        pie_chart = create_pie_charts(st.session_state.data)
        if pie_chart:
            rt_pie_chart.altair_chart(pie_chart, use_container_width=True)
        
        
        if not st.session_state.data.empty:
            latest_df = st.session_state.data.sort_values('datetime').drop_duplicates(['gas', 'checkpoint'], keep='last')
            latest_values.dataframe(
                latest_df[['gas', 'checkpoint', 'value', 'timestamp']].sort_values(['checkpoint', 'gas']), 
                use_container_width=True
            )
        
        raw_data_placeholder.dataframe(
            st.session_state.data.sort_values('datetime', ascending=False), 
            use_container_width=True
        )
        
        time.sleep(refresh_interval)
   
       
else:
    
    process_queue()
    
    
    line_chart = create_line_charts(st.session_state.data)
    if line_chart:
        rt_line_chart.altair_chart(line_chart, use_container_width=True)
    
   
    pie_chart = create_pie_charts(st.session_state.data)
    if pie_chart:
        rt_pie_chart.altair_chart(pie_chart, use_container_width=True)
    
    
    if not st.session_state.data.empty:
        latest_df = st.session_state.data.sort_values('datetime').drop_duplicates(['gas', 'checkpoint'], keep='last')
        latest_values.dataframe(
            latest_df[['gas', 'checkpoint', 'value', 'timestamp']].sort_values(['checkpoint', 'gas']), 
            use_container_width=True
        )
    

    raw_data_placeholder.dataframe(
        st.session_state.data.sort_values('datetime', ascending=False), 
        use_container_width=True
    )