# 📖 Sổ tay Triển khai Cụm Phân tán (Cấu trúc 4 nút)

Tài liệu này cung cấp hướng dẫn toàn diện để triển khai **Nền tảng Phân tích Luồng Video** trên một mạng vật lý phân tán (4 máy riêng biệt). Cấu trúc này mô phỏng môi trường sản xuất thực tế, đảm bảo giao tiếp mạng mạnh mẽ, cân bằng tải và khả năng chịu lỗi.

## 🏗️ Cấu trúc Cụm Vật lý

Chúng tôi sẽ phân phối khối lượng công việc trên 4 nút vật lý (Laptop/PC) như sau:

* **Nút 1 (192.168.1.101):** Môi giới Thông điệp (Kafka Cluster & Zookeeper) + Thu thập Dữ liệu (Producer).
* **Nút 2 (192.168.1.102):** Công nhân Xử lý Luồng 1 (Consumer).
* **Nút 3 (192.168.1.103):** Công cụ Lưu trữ (MongoDB Replica Set) + Công nhân Xử lý Luồng 2 (Consumer).
* **Nút 4 (192.168.1.104):** Lớp Hiển thị (Dashboard).

---

## ⚙️ Giai đoạn 1: Cấu hình Mạng & Bảo mật (Điều kiện tiên quyết)

Khả năng hiển thị mạng là rất quan trọng đối với một hệ thống phân tán. Thực hiện các bước sau trên **CẢ 4 NÚT**:

1. **Mạng Thống nhất:** Đảm bảo tất cả 4 nút được kết nối với cùng một mạng cục bộ (LAN), chẳng hạn như bộ định tuyến Wi-Fi chuyên dụng hoặc Điểm phát sóng di động.
2. **Quy tắc Tường lửa:** 
   * Trên Windows: Đi tới *Windows Defender Firewall -> Turn off Windows Defender Firewall* cho cả mạng Private và Public. Ngoài ra, hãy thêm các quy tắc inbound/outbound cho phép các cổng `9092, 9093, 27017-27019, 8501` một cách rõ ràng.
   * Trên Linux/Mac: Điều chỉnh `ufw` hoặc `pf` tương ứng.
3. **Tìm địa chỉ IP:** Chạy `ipconfig` (Windows) hoặc `ifconfig` (Linux/Mac) trên mỗi máy để xác định địa chỉ IPv4 của chúng. 
   *(Lưu ý: Các IP được liệt kê trong cấu trúc trên chỉ là ví dụ. Vui lòng thay thế chúng bằng IP thực tế của bạn).*

---

## 🛠️ Giai đoạn 2: Cung cấp Môi trường

1. **Phân phối Mã nguồn:** Đảm bảo mã nguồn giống hệt nhau được sao chép trên cả 4 nút.
2. **Cấu hình Kafka Listeners (CHỈ trên Nút 1):**
   * Chỉnh sửa `deployments/docker-compose.kafka.yml` trên Nút 1.
   * Ràng buộc các Kafka advertised listeners với địa chỉ IP vật lý của Nút 1 (không sử dụng `localhost`).
   ```yaml
   # Dưới dịch vụ kafka-1:
   KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://192.168.1.101:9092,INTERNAL://kafka-1:19092
   
   # Dưới dịch vụ kafka-2:
   KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://192.168.1.101:9093,INTERNAL://kafka-2:19093
   ```
3. **Cấu hình Biến Môi trường (Trên CẢ 4 NÚT):**
   * Sao chép `.env.example` thành `.env`.
   * Cập nhật các điểm cuối để định tuyến lưu lượng truy cập rõ ràng đến Nút 1 (Kafka) và Nút 3 (MongoDB).
   ```ini
   # Trỏ đến Nút 1 (Kafka Broker)
   KAFKA_BROKER=192.168.1.101:9092
   KAFKA_BROKERS=192.168.1.101:9092,192.168.1.101:9093

   # Trỏ đến Nút 3 (MongoDB Replica Set)
   MONGO_URI=mongodb://192.168.1.103:27017,192.168.1.103:27018,192.168.1.103:27019/?replicaSet=rs0
   ```

---

## 🚀 Giai đoạn 3: Điều phối Dịch vụ & Trình tự Khởi động

Để ngăn chặn tình trạng tranh chấp kết nối, hệ thống phải được khởi động theo trình tự rõ ràng sau:

### Bước 3.1: Khởi tạo Công cụ Lưu trữ (Nút 3)
* Thực thi trên **Nút 3**:
  ```bash
  docker-compose -f deployments/docker-compose.mongodb.yml up -d
  ```
* *Đợi 15-20 giây để quá trình bầu chọn Replica Set hoàn tất và nút Chính (Primary) được thiết lập.*

### Bước 3.2: Khởi tạo Môi giới Thông điệp (Nút 1)
* Thực thi trên **Nút 1**:
  ```bash
  docker-compose -f deployments/docker-compose.kafka.yml up -d
  ```
* *Khung xương hạ tầng phân tán hiện đã sẵn sàng hoạt động.*

### Bước 3.3: Triển khai các Công nhân Xử lý Luồng (Nút 2 & 3)
Chúng tôi chạy công cụ Xử lý trực tiếp qua Python để thể hiện nhật ký công nhân phân tán.
* Thực thi trên **Nút 2**:
  ```bash
  cd src/processing && pip install -r requirements.txt && python consumer.py
  ```
* Thực thi trên **Nút 3** (Cửa sổ dòng lệnh thứ hai):
  ```bash
  cd src/processing && pip install -r requirements.txt && python consumer.py
  ```
* *Nhật ký sẽ cho biết các công nhân đang chờ dữ liệu.*

### Bước 3.4: Triển khai Lớp Hiển thị (Nút 4)
* Thực thi trên **Nút 4** (Máy hiển thị):
  ```bash
  cd src/dashboard && pip install -r requirements.txt && streamlit run app.py
  ```
* *Giao diện bảng điều khiển sẽ tải, hiển thị trạng thái chờ.*

### Bước 3.5: Kích hoạt Thu thập Dữ liệu (Nút 1)
* Thực thi trên **Nút 1** (Cửa sổ dòng lệnh thứ hai):
  ```bash
  cd src/ingestion && pip install -r requirements.txt && python producer.py
  ```
* **Tiêu chí thành công:** Dữ liệu đo từ xa ngay lập tức chảy qua môi giới Kafka đến các công nhân phân tán trên Nút 2 & 3. Các chỉ số đã xử lý được lưu trữ bền bỉ trong MongoDB của Nút 3 và bảng điều khiển của Nút 4 trực quan hóa các phân tích trong thời gian thực.

---

## 🛡️ Giai đoạn 4: Thử nghiệm Khả năng chịu lỗi (Chaos Engineering)

Khi hệ thống đang xử lý dữ liệu với thông lượng cao nhất, bạn có thể thực hiện các thử nghiệm sau để xác nhận khả năng phục hồi của hệ thống.

### Kịch bản A: Chuyển đổi dự phòng Công nhân (Tái cân bằng tải)
1. **Hành động:** Trên Nút 2, dừng đột ngột tiến trình `consumer.py` đang chạy (Ctrl+C).
2. **Quan sát:** Đường ống xử lý không bị dừng lại.
3. **Giải thích kỹ thuật:** Giao thức Kafka Consumer Group phát hiện sự cố nhịp tim (heartbeat) của Nút 2. Nó tự động kích hoạt việc tái cân bằng phân vùng, gán lại khối lượng công việc của Nút 2 cho công nhân còn lại trên Nút 3. Không có dữ liệu nào bị mất.

### Kịch bản B: Tính khả dụng cao của Cơ sở dữ liệu (Bầu chọn Replica Set)
1. **Hành động:** Trên Nút 3, buộc dừng container MongoDB Chính:
   ```bash
   docker stop mongo1
   ```
2. **Quan sát:** Các công nhân xử lý luồng có thể ghi nhật ký lỗi hết thời gian kết nối tạm thời trong khoảng 3-5 giây. Ngay sau đó, quá trình xử lý chỉ số và cập nhật bảng điều khiển sẽ tiếp tục mượt mà.
3. **Giải thích kỹ thuật:** MongoDB Replica Set phát hiện việc mất `mongo1`. Một cuộc bầu chọn nhanh chóng diễn ra giữa các nút còn lại (`mongo2` và `mongo3`), thăng cấp một nút Chính mới. Logic của trình điều khiển cơ sở dữ liệu sẽ tự động kết nối lại với nút Chính mới, đảm bảo các hoạt động ghi liên tục.
