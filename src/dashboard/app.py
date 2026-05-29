import os
import time
import pandas as pd
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from pymongo import MongoClient
import logging

# ─────────────────────────────────────────────
# CẤU HÌNH & LOGGING
# ─────────────────────────────────────────────
logging.basicConfig(level=logging.INFO, format='%(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("Dashboard")

st.set_page_config(
    page_title="Hệ thống Phân tích Video Toàn cầu",
    page_icon="📺",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ─────────────────────────────────────────────
# DATA ACCESS LAYER (LỚP TRUY XUẤT DỮ LIỆU)
# ─────────────────────────────────────────────
class DashboardDataService:
    @staticmethod
    @st.cache_resource
    def get_database_collection():
        """Thiết lập kết nối tới MongoDB Replica Set."""
        mongo_uri = os.getenv("MONGO_URI", "mongodb://localhost:27017,localhost:27018,localhost:27019/?replicaSet=rs0")
        try:
            client = MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
            # Kiểm tra kết nối bằng lệnh ping
            client.admin.command('ping')
            return client["video_analytics"]["streaming_metrics"]
        except Exception as e:
            logger.error(f"Lỗi kết nối tới Storage Engine: {e}")
            return None

    @staticmethod
    def fetch_recent_metrics(collection, limit: int = 30):
        """Lấy các chỉ số thời gian thực mới nhất."""
        if collection is None:
            return None, None
            
        try:
            cursor = collection.find({}, {"_id": 0}).sort("timestamp", -1).limit(limit)
            data = list(cursor)
            
            if not data:
                return None, None
                
            latest_doc = data[0]
            # Đảo ngược để hiển thị theo trình tự thời gian trên biểu đồ đường
            df = pd.DataFrame(data[::-1])
            return df, latest_doc
        except Exception as e:
            logger.error(f"Lỗi khi truy vấn dữ liệu: {e}")
            return None, None

# ─────────────────────────────────────────────
# UI RENDERING LAYER (LỚP GIAO DIỆN)
# ─────────────────────────────────────────────
class DashboardUI:
    @staticmethod
    def apply_custom_css():
        """Inject CSS tùy chỉnh để làm đẹp giao diện."""
        st.markdown("""
            <style>
            /* Tùy chỉnh các thẻ Metric */
            [data-testid="stMetricValue"] {
                font-size: 1.8rem !important;
                color: #00d4ff;
            }
            [data-testid="stMetricLabel"] {
                font-size: 1rem !important;
                font-weight: 500;
            }
            .main {
                background-color: #0e1117;
            }
            /* Bo góc cho các block */
            div[data-testid="stVerticalBlock"] > div:has(div.stPlotlyChart) {
                background-color: #161b22;
                padding: 15px;
                border-radius: 10px;
                border: 1px solid #30363d;
                margin-bottom: 20px;
            }
            /* Thanh bên chuyên nghiệp */
            section[data-testid="stSidebar"] {
                background-color: #161b22;
                border-right: 1px solid #30363d;
            }
            /* Căn giữa Logo và các thành phần Sidebar */
            [data-testid="stSidebarContent"] {
                display: flex;
                flex-direction: column;
                align-items: center;
            }
            [data-testid="stSidebarContent"] .stImage {
                display: flex;
                justify-content: center;
                padding: 20px 0; /* Căn giữa dọc cho vùng logo */
            }
            [data-testid="stSidebarContent"] .stButton {
                display: flex;
                justify-content: center;
                width: 100%;
            }
            [data-testid="stSidebarContent"] .stButton button {
                width: 90% !important;
                background-color: #1f6feb;
                color: white;
                border: none;
                transition: 0.3s;
            }
            [data-testid="stSidebarContent"] .stButton button:hover {
                background-color: #388bfd;
                border: none;
            }
            [data-testid="stSidebarContent"] h1 {
                text-align: center;
                font-size: 1.4rem !important;
                margin-top: 10px;
            }
            h1, h2, h3 {
                color: #f0f6fc;
            }
            </style>
        """, unsafe_allow_html=True)

    @staticmethod
    def render_sidebar(connection_status: bool):
        """Hiển thị thanh bên với thông tin hệ thống."""
        with st.sidebar:
            # 1. Logo căn giữa tuyệt đối
            st.image("logo.png", width=200)
            
            # 2. Nút làm mới ngay dưới ảnh
            if st.button("🔄 Làm mới hệ thống"):
                st.rerun()
            
            st.markdown("---")
            st.title("Trạng thái")
            
            # Trạng thái kết nối
            if connection_status:
                st.success("🟢 Database: OK")
            else:
                st.error("🔴 Database: Lỗi")
            
            st.markdown("---")
            st.info("""
            **Thông tin phiên bản**
            - Phiên bản: 2.0 (Pro)
            - Tần suất: 2 giây/lần
            """)

    @staticmethod
    def render_header():
        st.title("🚀 Trung tâm Giám sát Luồng Video Toàn cầu")
        st.markdown("_Hệ thống giám sát đo lường từ xa (telemetry) theo thời gian thực trên các điểm thu nhận dữ liệu phân tán._")
        st.write("")

    @staticmethod
    def render_kpis(latest_data: dict):
        """Hiển thị các chỉ số quan trọng (KPIs)."""
        c1, c2, c3 = st.columns(3)
        
        with c1:
            val = latest_data.get("total_events_processed", 0)
            st.metric(label="Sự kiện đã xử lý (Batch mới nhất)", value=f"{val:,}")
            
        with c2:
            bitrate = latest_data.get('avg_bitrate_kbps', 0)
            st.metric(label="Bitrate trung bình toàn cầu", value=f"{bitrate:,.0f} kbps")
            
        with c3:
            buffer_pct = latest_data.get('avg_buffer_ratio', 0) * 100
            st.metric(label="Tỷ lệ bộ đệm (Trễ mạng)", value=f"{buffer_pct:.2f} %", delta=f"{buffer_pct:.2f}%", delta_color="inverse")
            
        st.markdown("---")

    @staticmethod
    def render_charts(df: pd.DataFrame, latest_data: dict):
        """Hiển thị các biểu đồ phân tích dữ liệu chuyên sâu."""
        row1_col1, row1_col2 = st.columns(2)

        with row1_col1:
            st.subheader("📍 Phân bổ địa lý người xem")
            regions_data = latest_data.get("region_counts", {})
            if regions_data:
                df_regions = pd.DataFrame(list(regions_data.items()), columns=["Khu vực", "Số lượng"])
                fig_pie = px.pie(
                    df_regions, names="Khu vực", values="Số lượng", 
                    hole=0.5, 
                    color_discrete_sequence=px.colors.sequential.Tealgrn,
                    template="plotly_dark"
                )
                fig_pie.update_layout(margin=dict(t=30, b=10, l=10, r=10), height=350)
                st.plotly_chart(fig_pie, use_container_width=True)

        with row1_col2:
            st.subheader("⚠️ Cảnh báo nghẽn mạng (Top Video Lag)")
            lagging = latest_data.get("lagging_videos", {})
            if lagging:
                df_lag = pd.DataFrame(list(lagging.items()), columns=["Video ID", "Số lần Buffer"]).sort_values(by="Số lần Buffer", ascending=False)
                fig_bar = px.bar(
                    df_lag.head(5), x="Video ID", y="Số lần Buffer", 
                    color="Số lần Buffer", 
                    color_continuous_scale="Reds",
                    template="plotly_dark"
                )
                fig_bar.update_layout(margin=dict(t=30, b=10, l=10, r=10), height=350)
                st.plotly_chart(fig_bar, use_container_width=True)
            else:
                st.success("Mạng lưới toàn cầu ổn định. Không phát hiện sự cố giật lag nghiêm trọng.")

        st.write("")
        st.subheader("📈 Biến động Bitrate toàn cầu (Thời gian thực)")
        
        # Biểu đồ đường Bitrate với style chuyên nghiệp
        fig_line = go.Figure()
        fig_line.add_trace(go.Scatter(
            x=df["timestamp"], y=df["avg_bitrate_kbps"],
            mode='lines+markers',
            name='Bitrate (kbps)',
            line=dict(color='#00d4ff', width=3),
            marker=dict(size=8, color='#ffffff', line=dict(width=2, color='#00d4ff')),
            fill='tozeroy',
            fillcolor='rgba(0, 212, 255, 0.1)'
        ))
        
        fig_line.update_layout(
            template="plotly_dark",
            xaxis_title="Thời gian",
            yaxis_title="Bitrate (kbps)",
            margin=dict(t=20, b=20, l=20, r=20),
            height=400,
            hovermode="x unified"
        )
        st.plotly_chart(fig_line, use_container_width=True)

# ─────────────────────────────────────────────
# CHƯƠNG TRÌNH CHÍNH (MAIN LOOP)
# ─────────────────────────────────────────────
def main():
    DashboardUI.apply_custom_css()
    
    collection = DashboardDataService.get_database_collection()
    db_connected = collection is not None
    
    DashboardUI.render_sidebar(db_connected)
    DashboardUI.render_header()
    
    # Khu vực hiển thị nội dung chính tự động cập nhật
    placeholder = st.empty()
    
    with placeholder.container():
        df, latest = DashboardDataService.fetch_recent_metrics(collection)
        
        if not db_connected:
            st.error("❌ Không thể kết nối tới Cơ sở dữ liệu. Vui lòng kiểm tra Docker containers (MongoDB Replica Set).")
            st.info("Gợi ý: Chạy lệnh `docker compose -f deployments/docker-compose.mongodb.yml up -d`")
        elif df is None or latest is None:
            with st.status("Đang chờ dữ liệu từ hệ thống xử lý...", expanded=True) as status:
                st.write("Đang kiểm tra Kafka Producers & Consumers...")
                st.write("Đang quét các bản ghi mới trong MongoDB...")
                time.sleep(1)
            st.warning("Chưa có dữ liệu mới. Đảm bảo các dịch vụ Ingestion và Processing đang hoạt động.")
        else:
            DashboardUI.render_kpis(latest)
            DashboardUI.render_charts(df, latest)

    # Cơ chế tự động làm mới (Mỗi 2 giây)
    time.sleep(2)
    st.rerun()

if __name__ == "__main__":
    main()
