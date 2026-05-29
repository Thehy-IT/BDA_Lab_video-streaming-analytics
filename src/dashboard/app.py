import os
import time
import pandas as pd
import streamlit as st
import plotly.express as px
from pymongo import MongoClient
import logging

# ─────────────────────────────────────────────
# CONFIGURATION & LOGGING
# ─────────────────────────────────────────────
logging.basicConfig(level=logging.INFO, format='%(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("Dashboard")

st.set_page_config(
    page_title="Global Video Analytics",
    page_icon="📺",
    layout="wide"
)

# ─────────────────────────────────────────────
# DATA ACCESS LAYER
# ─────────────────────────────────────────────
class DashboardDataService:
    @staticmethod
    @st.cache_resource
    def get_database_collection():
        """Establishes connection to the MongoDB Replica Set."""
        mongo_uri = os.getenv("MONGO_URI", "mongodb://localhost:27017,localhost:27018,localhost:27019/?replicaSet=rs0")
        try:
            client = MongoClient(mongo_uri, serverSelectionTimeoutMS=10000)
            # Trigger a ping to verify connection
            client.admin.command('ping')
            return client["video_analytics"]["streaming_metrics"]
        except Exception as e:
            logger.error(f"Failed to connect to Storage Engine: {e}")
            return None

    @staticmethod
    def fetch_recent_metrics(collection, limit: int = 30):
        """Fetches the latest time-series metrics."""
        if collection is None:
            return None, None
            
        cursor = collection.find({}, {"_id": 0}).sort("timestamp", -1).limit(limit)
        data = list(cursor)
        
        if not data:
            return None, None
            
        latest_doc = data[0]
        # Reverse to chronological order for line charts
        df = pd.DataFrame(data[::-1])
        return df, latest_doc

# ─────────────────────────────────────────────
# UI RENDERING LAYER
# ─────────────────────────────────────────────
class DashboardUI:
    @staticmethod
    def render_header():
        st.title("📺 Global Video Streaming Operations Center")
        st.markdown("Real-time distributed system monitoring telemetry across global ingestion points.")

    @staticmethod
    def render_kpis(latest_data: dict):
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric(label="Events Processed (Last Batch)", value=latest_data.get("total_events_processed", 0))
        with col2:
            st.metric(label="Global Avg Bitrate (kbps)", value=f"{latest_data.get('avg_bitrate_kbps', 0):.0f} kbps")
        with col3:
            buffer_pct = latest_data.get('avg_buffer_ratio', 0) * 100
            st.metric(label="Global Buffer Ratio (Lag)", value=f"{buffer_pct:.2f} %")
        st.markdown("---")

    @staticmethod
    def render_charts(df: pd.DataFrame, latest_data: dict):
        row1_col1, row1_col2 = st.columns(2)

        with row1_col1:
            st.subheader("Audience Geographic Distribution")
            regions_data = latest_data.get("region_counts", {})
            if regions_data:
                df_regions = pd.DataFrame(list(regions_data.items()), columns=["Region", "Count"])
                fig_pie = px.pie(df_regions, names="Region", values="Count", hole=0.4, color_discrete_sequence=px.colors.sequential.Teal)
                st.plotly_chart(fig_pie, use_container_width=True)

        with row1_col2:
            st.subheader("Network Anomalies (Top Lagging Videos)")
            lagging = latest_data.get("lagging_videos", {})
            if lagging:
                df_lag = pd.DataFrame(list(lagging.items()), columns=["Video ID", "Buffer Incidents"]).sort_values(by="Buffer Incidents", ascending=False)
                fig_bar = px.bar(df_lag.head(5), x="Video ID", y="Buffer Incidents", color="Buffer Incidents", color_continuous_scale="Reds")
                st.plotly_chart(fig_bar, use_container_width=True)
            else:
                st.success("Global network is stable. No severe buffering detected.")

        st.markdown("---")

        st.subheader("Real-time Global Bitrate Timeline")
        fig_line = px.line(df, x="timestamp", y=["avg_bitrate_kbps"], markers=True, title="Average Bitrate over Time")
        st.plotly_chart(fig_line, use_container_width=True)

# ─────────────────────────────────────────────
# MAIN APPLICATION LOOP
# ─────────────────────────────────────────────
def main():
    DashboardUI.render_header()
    
    collection = DashboardDataService.get_database_collection()
    
    # Placeholder for live refresh
    placeholder = st.empty()
    
    with placeholder.container():
        df, latest = DashboardDataService.fetch_recent_metrics(collection)
        
        if df is None:
            st.warning("⏳ Awaiting data from Processing Nodes... Ensure Kafka Brokers and Zookeeper are active.")
        else:
            DashboardUI.render_kpis(latest)
            DashboardUI.render_charts(df, latest)

    # Polling Mechanism (Every 2 seconds)
    time.sleep(2)
    st.rerun()

if __name__ == "__main__":
    main()