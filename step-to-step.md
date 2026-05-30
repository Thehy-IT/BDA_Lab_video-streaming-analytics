# 🚀 Sổ tay "Chiến Game" Video Streaming Analytics (Team HY & Anh Em)

Để cái Lab này chạy mượt, không ông nào bị "xì khói" máy, anh em mình làm theo đúng hướng dẫn này nhé. Tài liệu này viết cho dân mình nên cực kỳ dễ hiểu, cứ thế mà triển!

---

## 🏗️ 1. Sơ đồ tác chiến (Ai làm việc nấy)

Team mình có 3 Nút (Node), tui chia việc cho **Thuận, Rùa, HY** như sau:

| Thành viên     | Vị trí (Node)  | IP              | Cần cài      | Nhiệm vụ chính                                |
| :--------------- | :--------------- | :-------------- | :------------- | :----------------------------------------------- |
| **Thuận** | **Nút 1** | 10.56.152.94    | Docker, Python | "Đầu não" Kafka & "Máy phát" Producer       |
| **Rùa**   | **Nút 2** | `10.20.64.69` | Docker, Python | "Kho chứa" MongoDB & "Máy lọc 1" Consumer 1   |
| **HY**     | **Nút 3** | 10.229.172.19   | Python         | "Bảng tin" Dashboard & "Máy lọc 2" Consumer 2 |

## ⚙️ 2. Giai đoạn "Khởi động" (Tất cả phải làm)

Trước khi bấm nút, anh em làm ngay cho tui mấy việc "thủ tục" này:

1. **Dùng chung 1 Wi-Fi:** Tất cả kết nối vào cùng 1 mạng (tốt nhất là phát từ điện thoại cho ổn định).
2. **Tắt Firewall:** Windows Firewall là "kẻ thù" của kết nối mạng. Vào *Firewall* chọn *Turn off* hết đi (xong Lab nhớ bật lại không dính virus tui không chịu trách nhiệm đâu nha).
3. **Check IP:** Mở CMD gõ `ipconfig`. Lấy cái `IPv4 Address`. Ông nào IP khác bảng trên thì hú lên để cả team cùng sửa.

---

## 🛠️ 3. Cấu hình .env

Cả team copy file `.env.example` thành `.env`. Sau đó sửa nội dung theo đúng IP máy của **Thuận** và **Rùa**:

```ini
# Trỏ về máy của Thuận (Kafka)
KAFKA_BROKER=10.56.152.94:9092

# Trỏ về máy của Rùa (Mongo)
MONGO_URI=mongodb://10.20.64.69:27017,10.20.64.69:27018,10.20.64.69:27019/?replicaSet=rs0
```

---

## 🚀 4. Giai đoạn "Lên sàn" (Thứ tự cực kỳ quan trọng!)

Làm sai thứ tự là nó lỗi "lên bờ xuống ruộng" đó. Anh em cứ thong thả, ông trước xong thì ông sau mới làm.

### Bước 1: Rùa dựng "Kho hàng" (Nút 2)

**Rùa** mở terminal tại thư mục gốc:

```bash
docker-compose -f deployments/docker-compose.mongodb.yml up -d
```

*Hóng khoảng 30s cho Database nó tự bầu "Sếp" (Primary) xong đã nhé.*

### Bước 2: Thuận dựng "Trạm trung chuyển" (Nút 1)

**Thuận** check lại file `deployments/docker-compose.kafka.yml`, chỗ nào có IP thì sửa thành IP máy ông. Xong rồi quất:

```bash
docker-compose -f deployments/docker-compose.kafka.yml up -d
```

### Bước 3: Rùa & HY chạy "Máy lọc" (Nút 2 & 3)

Cả hai ông cùng vào thư mục `src/processing`, cài thư viện và chạy:

```bash

python consumer.py
```

*Thấy nó đứng im "Waiting for messages..." là ngon lành, đừng lo!*

### Bước 4: HY mở "Bảng tin" (Nút 3)

**HY** vào `src/dashboard`:

```bash
pip install -r requirements.txt
streamlit run app.py
```

*Nó sẽ hiện ra một cái link Web, mở lên xem cho sướng mắt.*

### Bước 5: Thuận "Khai hỏa" (Nút 1)

Cuối cùng, **Thuận** vào `src/ingestion` và bấm nút:

```bash
pip install -r requirements.txt
python producer.py
```

*Dữ liệu bắt đầu bay vèo vèo từ máy Thuận sang máy Rùa và HY rồi đó!*

---

## 🛡️ 5. Giai đoạn "Quậy phá" (Thử độ bền hệ thống)

Sau khi hệ thống đã chạy ổn định, anh em mình sẽ cùng làm "Hacker" để thử xem hệ thống có thực sự "bất tử" như quảng cáo không nhé:

### Kịch bản 1: Consumer "Hy sinh" (Lỗi xử lý)

- **Hành động:** **HY** tắt `consumer.py` ở máy Nút 3 (nhấn `Ctrl+C`).
- **Quan sát:**
  1. Check máy **Rùa** (Nút 2), cái Consumer ở đó vẫn phải chạy và nhận dữ liệu bình thường.
  2. Check **Dashboard** của **HY**, dữ liệu vẫn phải được cập nhật (dù có thể chậm hơn một chút vì mất đi 1 máy xử lý).
  3. **Kết luận:** Kafka đã tự động điều phối (rebalance) lại để máy của Rùa gánh hết phần việc của HY.

### Kịch bản 2: Database "Sập nguồn" (Lỗi lưu trữ)

- **Hành động:** **Rùa** tắt con Database chủ lực: `docker stop mongo1`.
- **Quan sát:**
  1. Đợi khoảng 10-15 giây. Dashboard có thể hơi lag nhẹ lúc đầu.
  2. Sau đó, dữ liệu phải tiếp tục hiện lên.
  3. **Rùa** gõ `docker exec -it mongo2 mongosh --eval "rs.status()"` để xem con nào vừa được bầu làm "Sếp" (Primary) mới.
  4. **Kết luận:** Cơ chế Replica Set của MongoDB đã cứu nguy, tự bầu sếp mới để hệ thống không bị gián đoạn.

### Kịch bản 3: Kafka "Mất sóng" (Lỗi truyền dẫn - Cực khó)

- **Hành động:** **Thuận** tắt 1 trong 2 Broker Kafka.
- **Quan sát:**
  1. Các máy Consumer sẽ báo lỗi kết nối trong vài giây nhưng sau đó sẽ tự kết nối lại vào Broker còn lại.
  2. Dữ liệu không được mất, chỉ bị chậm lại.
  3. **Kết luận:** Nhờ có nhiều Broker, hệ thống vẫn duy trì được luồng dữ liệu.

---

**Lời nhắn nhủ từ Trưởng nhóm HY:**

- "Quậy" xong thì nhớ bật lại hết để chụp ảnh làm báo cáo nhé anh em!
- Lỗi gì thì check cái IP đầu tiên. 90% lỗi là do sai IP.
- Python thì dùng bản 3.9 trở lên cho chắc.
- Bí quá thì hú tui ngay, tui hỗ trợ 24/7 (trừ lúc tui ngủ).

**Chúc anh em mình làm Lab điểm cao chót vót! 🚀🔥**
