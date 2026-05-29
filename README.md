# 📺 Nền tảng Phân tích Luồng Video Toàn cầu

Một đường ống dữ liệu phân tán, có khả năng chịu lỗi và thời gian thực được thiết kế để thu thập, xử lý và trực quan hóa dữ liệu đo từ xa (telemetry) từ các ứng dụng xem video trên toàn cầu. Hệ thống này thể hiện các nguyên tắc cốt lõi của kỹ thuật Dữ liệu lớn (Big Data), bao gồm khả năng mở rộng theo chiều ngang, tính khả dụng cao và xử lý luồng dữ liệu bền bỉ.

## 🏗️ Kiến trúc Hệ thống

Nền tảng được xây dựng dựa trên kiến trúc microservices hướng sự kiện, được tách biệt thành bốn lớp riêng biệt:

1. **Lớp Thu thập Dữ liệu (Mô phỏng Edge/Client)**

   * **Thành phần:** Trình tạo dữ liệu đo từ xa (Telemetry Producer) dựa trên Python.
   * **Vai trò:** Mô phỏng các sự kiện khách hàng đồng thời với thông lượng cao (trạng thái phát lại, số liệu đệm, điều chỉnh bitrate) bắt nguồn từ các thiết bị biên toàn cầu.
   * **Định tuyến:** Các sự kiện được tuần tự hóa và phân vùng một cách xác định vào các chủ đề (topics) Kafka (`play_events`, `quality_metrics`, `user_actions`) để tối ưu hóa việc tiêu thụ dữ liệu ở hạ nguồn.
2. **Môi giới Thông điệp / Tổng hợp Nhật ký (Log Aggregation)**

   * **Thành phần:** Cụm Apache Kafka (với Zookeeper).
   * **Vai trò:** Đóng vai trò là hệ thần kinh trung ương. Cung cấp nhật ký cam kết (commit logs) phân tán, bền bỉ và được phân vùng.
   * **Khả năng phục hồi:** Được cấu hình với Hệ số nhân bản (Replication Factor) = 2. Đảm bảo không mất dữ liệu và duy trì tính khả dụng liên tục ngay cả khi có sự cố phân vùng môi giới hoặc lỗi phần cứng.
3. **Lớp Xử lý Luồng (Stream Processing Layer)**

   * **Thành phần:** Các trình tiêu thụ Kafka (Kafka Consumers) dựa trên Python.
   * **Vai trò:** Thực hiện xử lý vi đợt (micro-batch) gần thời gian thực (NRT) (cửa sổ tumbling 5 giây). Tổng hợp dữ liệu thô thành các chỉ số có thể hành động (ví dụ: Bitrate trung bình, Tỷ lệ đệm, Tỷ lệ lỗi).
   * **Khả năng mở rộng:** Thiết kế phi trạng thái (stateless) cho phép mở rộng theo chiều ngang. Các trình tiêu thụ trong cùng một `group_id` sẽ tự động cân bằng lại các phân vùng, cung cấp khả năng phân phối tải động và chuyển đổi dự phòng tức thì.
4. **Lớp Lưu trữ Phân tán & Trực quan hóa**

   * **Lưu trữ:** Bộ bản sao MongoDB (1 Chính, 2 Phụ). Cung cấp Tính khả dụng cao (HA) thông qua việc tự động chuyển đổi dự phòng/bầu chọn, đảm bảo lưu trữ mạnh mẽ cho các chỉ số tổng hợp.
   * **Trực quan hóa:** Streamlit + Plotly. Một bảng điều khiển hoạt động tương tác truy vấn lớp lưu trữ để hiển thị thông tin chi tiết theo thời gian thực.

---

## 🛠️ Công nghệ Sử dụng (Stack)

* **Ngôn ngữ lập trình:** Python 3.9+ (kafka-python, pymongo, pandas)
* **Cơ sở hạ tầng & Container hóa:** Docker, Docker Compose
* **Truyền luồng sự kiện:** Apache Kafka, Apache Zookeeper
* **Cơ sở dữ liệu phân tán:** MongoDB (Chế độ Replica Set)
* **Frontend/Dashboard:** Streamlit, Plotly

---

## 🚀 Hướng dẫn Phát triển Cục bộ (Máy đơn)

Phần này hướng dẫn triển khai toàn bộ hệ thống trên một máy phát triển cục bộ duy nhất. Để triển khai cụm phân tán đa nút (4 máy), vui lòng tham khảo [Sổ tay triển khai cụm phân tán](step-to-step.md).

### 1. Điều kiện tiên quyết

* Đã cài đặt và đang chạy [Docker Desktop](https://www.docker.com/products/docker-desktop/).
* Git để quản lý phiên bản.
* (Tùy chọn) Python 3.9+ nếu bạn muốn chạy các dịch vụ trực tiếp bên ngoài container.

### 2. Cấu hình Môi trường

Sao chép kho lưu trữ và khởi tạo cấu hình môi trường:

```bash
git clone <repository_url>
cd BDA_Lab_video-streaming-analytics
cp .env.example .env
```

*(Tệp `.env.example` mặc định đã được cấu hình sẵn để thực thi trên `localhost`).*

### 3. Khởi động nhanh (Tự động)

Chúng tôi đã cung cấp các tệp lệnh tiện ích để điều phối toàn bộ nền tảng chỉ bằng một lệnh duy nhất.

**Dành cho Windows:**

```cmd
.\scripts\start_all.bat
```

**Dành cho Linux/Mac:**

```bash
chmod +x scripts/start_all.sh
./scripts/start_all.sh
```

*Tệp lệnh này tự động thiết lập mạng Docker, khởi tạo hạ tầng Kafka và MongoDB, và khởi chạy các dịch vụ ứng dụng.*

Dừng và xoá các Container cũ:

`docker-compose -f deployments/docker-compose.mongodb.yml -f deployments/docker-compose.kafka.yml -f deployments/docker-compose.services.yml down`

Khởi động lại và Build lại:
`docker-compose -f deployments/docker-compose.mongodb.yml -f deployments/docker-compose.kafka.yml -f deployments/docker-compose.services.yml up -d --build`

### 4. Xác minh Dịch vụ

Sau khi cụm đã hoạt động, hãy truy cập các giao diện sau:

* **Bảng điều khiển hoạt động:** [http://docker localhost:8501](http://localhost:8501)
* **Giao diện quản lý Kafka (Theo dõi cụm):** [http://localhost:8080](http://localhost:8080)

### 5. Dừng hệ thống

Để dừng cụm một cách an toàn và loại bỏ các container/mạng liên quan:
**Windows:** `.\scripts\stop_all.bat`
**Linux/Mac:** `./scripts/stop_all.sh`

---

## 📂 Cấu trúc Dự án

```text
.
├── deployments/         # Hạ tầng dưới dạng mã (Docker Compose cho Kafka, MongoDB, Dịch vụ)
├── docs/                # Sơ đồ kiến trúc và tài liệu kỹ thuật
├── scripts/             # Các tệp lệnh tự động hóa cho Trải nghiệm Nhà phát triển (DX)
├── src/                 # Mã nguồn ứng dụng
│   ├── ingestion/       # Trình tạo dữ liệu đo từ xa
│   ├── processing/      # Công cụ xử lý luồng
│   └── dashboard/       # Giao diện trực quan hóa thời gian thực
├── tests/               # Bộ kiểm thử tự động (Unit/Integration)
├── .env.example         # Mẫu biến môi trường
├── step-to-step.md      # Sổ tay triển khai cụm phân tán
└── README.md            # Tổng quan dự án (Tệp này)
```
