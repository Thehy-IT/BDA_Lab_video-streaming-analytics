Để demo thành công trên 4 máy tính vật lý (4 laptop của 4 thành viên), **vấn đề mạng LAN và IP là quan trọng nhất**. Chỉ cần sai 1 IP hoặc quên tắt tường lửa, hệ thống sẽ không thể giao tiếp với nhau.

Dưới đây là hướng dẫn chi tiết từng bước (Step-by-step).

---

### PHẦN 0: CHUẨN BỊ MẠNG (Bắt buộc làm đầu tiên)

1. **Dùng chung 1 mạng:** Bật 1 cục phát Wifi riêng (hoặc dùng 1 điện thoại phát 4G). Yêu cầu cả 4 laptop kết nối vào Wifi này.
2. **Tắt Tường lửa (Firewall):** Trên cả 4 máy (đặc biệt là Windows), vào *Windows Defender Firewall -> Turn off Windows Defender Firewall* (cho cả Private và Public network). Nếu không tắt, các máy sẽ chặn cổng của nhau.
3. **Lấy địa chỉ IP của 4 máy:** Mở Terminal/CMD gõ `ipconfig` (Windows) hoặc `ifconfig` (Mac). Ghi lại IPv4 của 4 máy.
   *Giả sử ta có các IP sau để làm ví dụ trong hướng dẫn này:*
   * **Máy 1 (Thành viên A):** `192.168.1.101` THUẬN
   * **Máy 2 (Thành viên B):** `192.168.1.102` DƯƠNG
   * **Máy 3 (Thành viên C):** `192.168.1.103` RÙA
   * **Máy 4 (Thành viên D):** `192.168.1.104` HY
4. **Đồng bộ Code:** Cả 4 máy đều phải tải (clone) thư mục code `video-streaming-analytics` về máy giống hệt nhau.

---

### PHẦN 1: CẤU HÌNH LẠI CODE CHO PHÙ HỢP VỚI 4 MÁY

Vì chúng ta chạy trên 4 máy khác nhau, không còn là `localhost` nữa. Cần phải sửa một chút cấu hình:

**👉 Sửa tại Máy 1 (Máy chạy Kafka):**
Mở file `deployments/docker-compose.kafka.yml` trên Máy 1. Tìm dòng `KAFKA_ADVERTISED_LISTENERS` của cả `kafka-1` và `kafka-2`, sửa chữ `localhost` thành IP của Máy 1 (`192.168.1.101`).

```yaml
# Trong kafka-1:
KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://192.168.1.101:9092,INTERNAL://kafka-1:19092
# Trong kafka-2:
KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://192.168.1.101:9093,INTERNAL://kafka-2:19093
```

**👉 Sửa file `.env` trên TẤT CẢ 4 MÁY:**
Mở file `.env` (hoặc copy từ `.env.example`) trên cả 4 máy và điền đúng IP của Máy 1 (Kafka) và Máy 3 (MongoDB) vào:

```ini
# Trỏ về Máy 1
KAFKA_BROKER=192.168.1.101:9092
KAFKA_BROKERS=192.168.1.101:9092,192.168.1.101:9093

# Trỏ về Máy 3
MONGO_URI=mongodb://192.168.1.103:27017,192.168.1.103:27018,192.168.1.103:27019/?replicaSet=rs0
```

---

### PHẦN 2: THỨ TỰ KHỞI ĐỘNG (Làm theo đúng thứ tự 1 -> 5)

Để hệ thống không bị lỗi "Không tìm thấy kết nối", bạn phải hô hào team khởi động theo thứ tự sau:

#### 🟢 Bước 1: Máy 3 khởi động Storage (Database)

- Thành viên Máy 3 mở Terminal tại thư mục gốc:
  ```bash
  docker-compose -f deployments/docker-compose.mongodb.yml up -d
  ```
- Đợi khoảng 20 giây để 3 node MongoDB bầu cử Master.

#### 🟢 Bước 2: Máy 1 khởi động Ingestion (Kafka Broker)

- Thành viên Máy 1 mở Terminal tại thư mục gốc:
  ```bash
  docker-compose -f deployments/docker-compose.kafka.yml up -d
  ```
- *Lúc này Hạ tầng phân tán đã sẵn sàng.*

#### 🟢 Bước 3: Máy 2 & Máy 3 khởi động Processing Nodes (Consumer)

- Thành viên Máy 2 mở Terminal:
  ```bash
  cd src/processing
  python consumer.py
  ```
- Thành viên Máy 3 mở một Terminal thứ hai (chạy song song 2 consumer để load balancing):
  ```bash
  cd src/processing
  python consumer.py
  ```
- *(Lúc này màn hình Consumer sẽ báo: "Trạng thái chờ: Không có luồng dữ liệu mới..." vì chưa có ai gửi data).*

#### 🟢 Bước 4: Máy 4 khởi động Dashboard (Visualization)

- Cắm dây máy chiếu vào Máy 4. Thành viên Máy 4 mở Terminal:
  ```bash
  cd src/dashboard
  streamlit run app.py
  ```
- Màn hình máy chiếu sẽ hiện Dashboard lên, báo trạng thái "Đang chờ dữ liệu gửi về...".

#### 🟢 Bước 5: Máy 1 Bắn Dữ liệu (Action!)

- Khi giáo viên đã nhìn thấy màn hình Dashboard, bạn (Lead) hô Máy 1 bắt đầu sinh dữ liệu:
  ```bash
  cd src/ingestion
  python producer.py
  ```
- **KẾT QUẢ TRÊN MÁY CHIẾU:** Ngay lập tức, Máy 2 và Máy 3 báo `✅ Đã xử lý...`. Biểu đồ trên màn hình Máy 4 bắt đầu nhảy múa liên tục theo thời gian thực! (Ăn điểm chỗ này).

---

### PHẦN 3: KỊCH BẢN DEMO FAULT TOLERANCE (Thuyết trình trên bảng)

Khi data đang chảy rần rần, bạn thuyết trình: *"Thưa thầy, sau đây nhóm em xin demo tính năng Chịu lỗi (Fault Tolerance) của hệ thống phân tán".*

**Kịch bản 1: Giả lập chết Processing Node (Chết Máy 2)**

1. Bạn bảo thành viên Máy 2 **bấm Ctrl + C tắt ngang `consumer.py`**.
2. Chỉ lên máy chiếu: *"Thầy có thể thấy luồng dữ liệu không hề bị đứng"*.
3. Giải thích: Vì Máy 3 đang chạy `consumer.py` với cùng `CONSUMER_GROUP_ID`, Kafka đã tự động đẩy toàn bộ công việc của Máy 2 sang cho Máy 3 gánh. Data không bị mất.

**Kịch bản 2: Giả lập chết Storage Master (Chết Máy 3)**

1. Cái này cực kỳ ấn tượng. Bạn bảo Máy 3 gõ lệnh tắt node Primary của Database:
   ```bash
   docker stop mongo1
   ```
2. Mọi người sẽ thấy Consumer khựng lại báo lỗi kết nối đỏ chót khoảng 3 giây.
3. Chỉ lên máy chiếu: 3 giây sau, Consumer xanh lại, biểu đồ tiếp tục nhảy.
4. Giải thích: *"Khi node Master (mongo1) bị sập/rút dây mạng, cụm Replica Set đã tự động vote cho Secondary (mongo2) lên làm Master mới. Dữ liệu tiếp tục được ghi vào mà hệ thống tổng thể không bị chết"*.
