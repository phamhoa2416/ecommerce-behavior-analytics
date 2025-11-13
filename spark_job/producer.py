import pandas as pd
from kafka import KafkaProducer
import json
from datetime import datetime, timedelta
import time
import argparse
import sys

# === ARGUMENT PARSER ===
parser = argparse.ArgumentParser(description="Replay Kafka data from specific date")
parser.add_argument(
    '--date',
    type=str,
    required=True,
    help='Date to replay (YYYY-MM-DD), e.g., 2019-10-01'
)
args = parser.parse_args()

# Chuyển thành datetime.date
try:
    TARGET_DATE = datetime.strptime(args.date, '%Y-%m-%d').date()
except ValueError:
    print("Lỗi: Định dạng ngày phải là YYYY-MM-DD")
    sys.exit(1)

# === CẤU HÌNH ===
KAFKA_BOOTSTRAP = 'localhost:9092'
TOPIC = 'raw_data_topic'
CSV_FILE = './data/2019-Oct.csv'
CHUNK_SIZE = 10000
SPEEDUP_FACTOR = 5
START_SIMULATION = datetime(2019, 10, 1, 0, 0, 0)

# === KHỞI TẠO PRODUCER ===
def create_producer():
    for i in range(5):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                acks='all',
                retries=3,
                batch_size=16384,
                linger_ms=5
            )
            print(f"Kết nối Kafka thành công: {KAFKA_BOOTSTRAP}")
            return producer
        except Exception as e:
            print(f"Lần {i+1}/5: Không kết nối được Kafka: {e}")
            time.sleep(3)
    print("KHÔNG thể kết nối Kafka!")
    sys.exit(1)

producer = create_producer()

print(f"\nBắt đầu replay dữ liệu cho ngày: {TARGET_DATE}")

base_event_time = None
found_first_match = False

try:
    for chunk in pd.read_csv(CSV_FILE, chunksize=CHUNK_SIZE):
        # Chuyển event_time
        chunk['event_time'] = pd.to_datetime(chunk['event_time'], errors='coerce')
        chunk = chunk.dropna(subset=['event_time'])
        if chunk.empty:
            continue

        # Lấy ngày của row đầu và cuối
        first_time = chunk['event_time'].iloc[0]
        last_time = chunk['event_time'].iloc[-1]
        first_date = first_time.date()
        last_date = last_time.date()

        print(f"Chunk: {first_time} → {last_time} | Ngày: {first_date} → {last_date}")

        # === KIỂM TRA TIÊU CHÍ ===
        if not found_first_match:
            # Chưa tìm thấy → bỏ qua nếu không có ngày cần tìm
            if TARGET_DATE > first_date:
                print("→ Chưa đến ngày cần tìm, bỏ qua.")
                continue
            if TARGET_DATE < last_date:
                break
            # Nếu TARGET_DATE nằm trong [first_date, last_date] → bắt đầu
            if first_date >= TARGET_DATE >= last_date:
                print(f"→ TÌM THẤY! Bắt đầu gửi từ chunk này.")
                found_first_match = True
                base_event_time = first_time
            else:
                continue
        else:
            # Đã bắt đầu → kiểm tra xem chunk này còn dữ liệu ngày TARGET_DATE không
            if TARGET_DATE > first_date:
                print(f"→ Không còn dữ liệu ngày {TARGET_DATE}, dừng.")
                break

        # === GỬI DỮ LIỆU TRONG CHUNK ===
        for _, row in chunk.iterrows():
            event_time = row['event_time']
            event_date = event_time.date()

            # Chỉ gửi nếu cùng ngày
            if event_date != TARGET_DATE:
                continue

            # Tính thời gian mô phỏng
            if base_event_time is None:
                base_event_time = event_time
            delta_seconds = (event_time - base_event_time).total_seconds()
            simulated_time = START_SIMULATION + timedelta(seconds=delta_seconds * SPEEDUP_FACTOR)

            # Đợi đến thời điểm
            while datetime.now() < simulated_time:
                time.sleep(0.001)

            # Gửi
            data = row.to_dict()
            data['event_time'] = event_time.isoformat()
            producer.send(TOPIC, value=data)
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Sent: {event_time}")

    producer.flush()

except KeyboardInterrupt:
    print("\nDừng bởi người dùng.")
except Exception as e:
    print(f"Lỗi: {e}")
finally:
    producer.close()
    print("Đã đóng Kafka producer.")