import os
import time
import pandas as pd
import streamlit as st
import plotly.express as px
from pymongo import MongoClient

# ─────────────────────────────────────────────
# CẤU HÌNH GIAO DIỆN STREAMLIT
# ─────────────────────────────────────────────
st.set_page_config(
    page_title="Global Video Analytics",
    page_icon="📺",
    layout="wide"
)

st.title("📺 Trạm giám sát Video Streaming Toàn Cầu")
st.markdown("Hệ thống phân tán thu thập, xử lý và hiển thị thông số lượng người xem theo thời gian thực.")

# ─────────────────────────────────────────────
# KẾT NỐI DISTRIBUTED STORAGE (MONGODB REPLICA SET)
# ─────────────────────────────────────────────
@st.cache_resource
def get_database():
    # URI kết nối thẳng vào cụm Replica Set của MongoDB (3 node)
    mongo_uri = os.getenv("MONGO_URI", "mongodb://localhost:27017,localhost:27018,localhost:27019/?replicaSet=rs0")
    client = MongoClient(mongo_uri, serverSelectionTimeoutMS=10000)
    return client["video_analytics"]["streaming_metrics"]

collection = get_database()

# ─────────────────────────────────────────────
# HÀM LẤY VÀ XỬ LÝ DỮ LIỆU
# ─────────────────────────────────────────────
def fetch_data(limit=30):
    """Lấy 30 bản ghi mới nhất từ MongoDB và chuyển thành DataFrame Pandas"""
    # Lấy dữ liệu sắp xếp theo thời gian mới nhất (timestamp giảm dần)
    cursor = collection.find({}, {"_id": 0}).sort("timestamp", -1).limit(limit)
    data = list(cursor)
    
    if len(data) == 0:
        return None, None
    
    # Bản ghi mới nhất dùng cho các thẻ Thống kê (KPIs)
    latest_doc = data[0]
    
    # Sắp xếp lại dataframe theo chiều thời gian tăng dần để vẽ biểu đồ line
    df = pd.DataFrame(data[::-1]) 
    return df, latest_doc

# ─────────────────────────────────────────────
# VẼ GIAO DIỆN (DASHBOARD LAYOUT)
# ─────────────────────────────────────────────
# Vùng chứa Placeholder để có thể tự động refresh nội dung
dashboard_placeholder = st.empty()

with dashboard_placeholder.container():
    df, latest = fetch_data()

    if df is None:
        st.warning("⏳ Đang chờ dữ liệu gửi về từ Processing Nodes... Hãy chắc chắn Producer và Consumer đang chạy.")
    else:
        # 1. Thẻ KPIs (Chỉ số tổng quan)
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric(label="Lưu lượng Events/batch", value=latest.get("total_events_processed", 0))
        with col2:
            st.metric(label="Bitrate trung bình (kbps)", value=f"{latest.get('avg_bitrate_kbps', 0)} kbps")
        with col3:
            # Nếu tỷ lệ buffer (lag) lớn hơn 5% thì hiện màu đỏ cảnh báo
            buffer_pct = latest.get('avg_buffer_ratio', 0) * 100
            st.metric(label="Tỷ lệ Giật/Lag trung bình", value=f"{buffer_pct:.2f} %")

        st.markdown("---")

        # 2. Hàng Biểu đồ thứ nhất
        row1_col1, row1_col2 = st.columns(2)

        with row1_col1:
            st.subheader("Bản đồ phân bổ người xem")
            regions_data = latest.get("region_counts", {})
            if regions_data:
                df_regions = pd.DataFrame(list(regions_data.items()), columns=["Khu vực", "Số lượng"])
                fig_pie = px.pie(df_regions, names="Khu vực", values="Số lượng", hole=0.4, color_discrete_sequence=px.colors.sequential.Teal)
                st.plotly_chart(fig_pie, use_container_width=True)

        with row1_col2:
            st.subheader("Cảnh báo Video giật lag (Top Buffer)")
            lagging = latest.get("lagging_videos", {})
            if lagging:
                df_lag = pd.DataFrame(list(lagging.items()), columns=["Video ID", "Số lần gặp lỗi Buffer"]).sort_values(by="Số lần gặp lỗi Buffer", ascending=False)
                fig_bar = px.bar(df_lag.head(5), x="Video ID", y="Số lần gặp lỗi Buffer", color="Số lần gặp lỗi Buffer", color_continuous_scale="Reds")
                st.plotly_chart(fig_bar, use_container_width=True)
            else:
                st.success("Hệ thống mạng ổn định, không có video nào bị lag!")

        st.markdown("---")

        # 3. Hàng Biểu đồ thứ hai (Timeline)
        st.subheader("Biến động chất lượng mạng (Real-time Timeline)")
        fig_line = px.line(df, x="timestamp", y=["avg_bitrate_kbps"], markers=True, title="Bitrate tổng toàn cầu theo thời gian")
        st.plotly_chart(fig_line, use_container_width=True)

# ─────────────────────────────────────────────
# TỰ ĐỘNG REFRESH SAU MỖI 2 GIÂY
# ─────────────────────────────────────────────
time.sleep(2)
st.rerun()