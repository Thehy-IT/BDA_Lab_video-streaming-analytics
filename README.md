# 📺 Global Video Streaming Analytics (Distributed System Demo)

Dự án Xây dựng hệ thống phân tán (Distributed System) để thu thập, xử lý và trực quan hóa dữ liệu người xem video toàn cầu theo thời gian thực. Hệ thống mô phỏng luồng dữ liệu lớn (Big Data) với khả năng chịu lỗi và mở rộng cao.

## 🏗️ Kiến trúc Hệ thống (System Architecture)

Hệ thống được thiết kế theo mô hình Pipeline dữ liệu gồm 4 tầng chính:

1. **Data Ingestion (Tầng Thu thập):**

   * **Component:** Python Producer.
   * **Chức năng:** Giả lập hàng nghìn người xem video, sinh ra các sự kiện (play, pause, buffer, bitrate change) và gửi vào cụm Kafka.
   * **Phân phối:** Dữ liệu được định tuyến vào các Topic khác nhau (`play_events`, `quality_metrics`, `user_actions`) dựa trên loại sự kiện.
2. **Message Broker (Hệ thống Hàng đợi):**

   * **Component:** Apache Kafka Cluster (2 Brokers).
   * **Chức năng:** Lưu trữ trung gian dữ liệu với cơ chế Replication Factor = 2, đảm bảo dữ liệu không bị mất ngay cả khi một Broker gặp sự cố.
   * **Quản lý:** Kafka UI (Port 8080) giúp theo dõi luồng dữ liệu trực quan.
3. **Stream Processing (Tầng Xử lý):**

   * **Component:** Python Consumer (Micro-batching).
   * **Chức năng:** Tiêu thụ dữ liệu từ Kafka, gom nhóm (batching) mỗi 5 giây để tính toán các chỉ số: Tổng số event, Bitrate trung bình, Tỷ lệ giật lag (buffer), Top video bị lỗi.
   * **Mở rộng:** Có thể chạy nhiều Consumer Node trong cùng một `group_id` để chia sẻ tải (Load Balancing).
4. **Distributed Storage & Visualization (Lưu trữ & Trực quan hóa):**

   * **Storage:** MongoDB Replica Set (3 Nodes). Đảm bảo tính sẵn sàng cao (High Availability), tự động bầu chọn Master mới nếu có lỗi.
   * **Dashboard:** Streamlit + Plotly. Cập nhật biểu đồ thời gian thực từ dữ liệu đã xử lý trong MongoDB.

---

## 🛠️ Công nghệ sử dụng (Tech Stack)

* **Ngôn ngữ:** Python (Kafka-python, Pymongo, Pandas).
* **Infrastructure:** Docker & Docker Compose.
* **Message Broker:** Apache Kafka & Zookeeper.
* **Database:** MongoDB (Replica Set).
* **Visualization:** Streamlit, Plotly.

---

## 🚀 Hướng dẫn chạy hệ thống (Local)

### 1. Chuẩn bị môi trường

* Cài đặt [Docker Desktop](https://www.docker.com/products/docker-desktop/).
* Cài đặt Python 3.9+ (nếu muốn chạy các dịch vụ bên ngoài Docker).

### 2. Thiết lập cấu hình

Sao chép file cấu hình mẫu và chỉnh sửa nếu cần:

```bash
cp .env.example .env
```

### 3. Khởi động hạ tầng (Infrastructure)

Mở terminal và chạy các cụm phân tán:

**Bước A: Chạy Kafka Cluster**

```bash
docker-compose -f deployments/docker-compose.kafka.yml up -d
```

*Kiểm tra Kafka UI tại: http://localhost:8080*

**Bước B: Chạy MongoDB Replica Set**

```bash
docker-compose -f deployments/docker-compose.mongodb.yml up -d
```

*Lưu ý: Đợi khoảng 10-20 giây để container `mongo-setup` hoàn tất việc cấu hình Replica Set.*

### 4. Chạy các dịch vụ (Services)

Bạn có thể chạy trực tiếp bằng Python hoặc dùng Docker.

#### Cách 1: Chạy bằng Python (Khuyên dùng khi Dev)

Mở 3 terminal riêng biệt:

* **Terminal 1 (Ingestion):**
  ```bash
  cd src/ingestion
  pip install -r requirements.txt
  python producer.py
  ```
* **Terminal 2 (Processing):**
  ```bash
  cd src/processing
  pip install -r requirements.txt
  python consumer.py
  ```
* **Terminal 3 (Dashboard):**
  ```bash
  cd src/dashboard
  pip install -r requirements.txt
  streamlit run app.py
  ```

#### Cách 2: Chạy toàn bộ bằng Docker (Production mode)

Hệ thống đã có sẵn cấu hình Docker Compose để chạy các dịch vụ:

```bash
docker-compose -f deployments/docker-compose.services.yml up -d --build
```

*Lưu ý: Phương pháp này yêu cầu tầng Hạ tầng (Kafka & MongoDB) phải đang chạy và chung network `streaming_network`.*


docker-compose -f deployments/docker-compose.mongodb.yml -f deployments/docker-compose.kafka.yml -f deployments/docker-compose.services.yml up -d

---

## 📂 Cấu trúc thư mục

```text
.
├── deployments/            # File cấu hình Docker (Kafka, MongoDB)
├── docs/                   # Tài liệu hướng dẫn và sơ đồ kiến trúc
├── src/
│   ├── ingestion/          # Source code Producer (Đẩy dữ liệu)
│   ├── processing/         # Source code Consumer (Xử lý dữ liệu)
│   └── dashboard/          # Giao diện hiển thị (Streamlit)
├── .env.example            # File mẫu cấu hình biến môi trường
└── README.md               # Tài liệu dự án
```

## 🛡️ Tính năng nổi bật

* **Fault Tolerance:** Hệ thống vẫn hoạt động bình thường nếu 1 Kafka Broker hoặc 1 MongoDB Node bị sập.
* **Scalability:** Có thể dễ dàng tăng số lượng Consumer để xử lý lượng dữ liệu lớn hơn.
* **Real-time:** Độ trễ (Latency) thấp, dữ liệu được cập nhật lên Dashboard sau mỗi 5 giây.

## 👥 Nhóm thực hiện

* **Thành viên 1:** [Tên] - [MSSV]
* **Thành viên 2:** [Tên] - [MSSV]
* **Thành viên 3:** [Tên] - [MSSV]
* **Thành viên 4:** [Tên] - [MSSV]
