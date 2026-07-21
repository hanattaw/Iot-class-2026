import json
import numpy as np
import time
from datetime import datetime, timezone
from kafka import KafkaConsumer
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

# 💡 เพื่อความง่ายในการเริ่มปฏิบัติการของนักศึกษา สคริปต์นี้ใช้โมเดลจำลองตัวประมวลผลเชิงลึก
# สำหรับการทำงานระดับ Enterprise นักศึกษาเพียงแค่นำโมเดลที่เทรนจริงจาก Keras/TensorFlow 
# มาโหลดผ่าน tf.keras.models.load_model('lstm_model.h5') ในจุดนี้ได้ทันที

# ==================================================================
#  1. ส่วนกำหนดค่าเชื่อมต่อ (Configurations)
# ==================================================================
KAFKA_BOOTSTRAP_SERVERS = ["localhost:9092"]
KAFKA_TOPIC = "university.sensors.telemetry"

INFLUX_URL = "http://localhost:8086"
INFLUX_TOKEN = "mytoken"
INFLUX_ORG = "my-org"
INFLUX_BUCKET = "iot_data_stream"

# ขนาดความยาวของข้อมูลประวัติย้อนหลังที่โมเดล LSTM ต้องการ (Window Size)
# สมมติส่งข้อมูลทุกๆ 5 นาที ย้อนหลัง 12 จุดจะเท่ากับ 1 ชั่วโมงล่าสุด
WINDOW_SIZE = 12 
PREDICTION_OFFSET_MINUTES = 30 # ระยะเวลาทำนายล่วงหน้า (นาที)

# ==================================================================
#  2. โครงสร้างหน่วยความจำประวัติย้อนหลังรายเซนเซอร์ (Per-Sensor Sliding Window)
# ==================================================================
# โครงสร้าง: { "ID_68123456789": [25.1, 25.3, 25.4, ...] }
sensor_history_buffers = {}

# ==================================================================
#  3. จำลองฟังก์ชันเรียกประมวลผลทำนายด้วยโมเดล LSTM
# ==================================================================
def run_lstm_inference(history_sequence):
    """
    ฟังก์ชันส่งข้อมูลประวัติเข้าสู่โครงข่าย LSTM
    ในโปรเจกต์จริงของนักศึกษา:
    --------------------------------------------------------------
    # 1. ปรับสเกลข้อมูลให้อยู่ระหว่าง 0 ถึง 1 ตาม MinMaxScaler ที่ใช้ตอนเทรน
    scaled_seq = scaler.transform(np.array(history_sequence).reshape(-1, 1))
    # 2. ปรับ Shape ให้เข้ากับ LSTM Input [batch_size, time_steps, features]
    model_input = np.reshape(scaled_seq, (1, WINDOW_SIZE, 1))
    # 3. สั่งคำนวณทำนายและแปลงค่าสเกลกลับเป็นหน่วยอุณหภูมิจริง (°C)
    predicted_scaled = keras_model.predict(model_input)
    predicted_temp = scaler.inverse_transform(predicted_scaled)[0][0]
    return round(predicted_temp, 2)
    --------------------------------------------------------------
    """
    # จำลองอัลกอริทึมการคำนวณแนวโน้มเชิงเส้นเพื่อพยากรณ์ทิศทาง (Linear Trend Projection + Noise)
    # เพื่อให้นักศึกษาเห็นทิศทางการสวิงของกราฟล่วงหน้าได้ใกล้เคียงความจริง
    x = np.arange(len(history_sequence))
    y = np.array(history_sequence)
    slope, intercept = np.polyfit(x, y, 1) # หาความลาดชันของอุณหภูมิใน 1 ชั่วโมงล่าสุด
    
    # คำนวณทำนายล่วงหน้าโดยใช้ค่าความเร่งอุณหภูมิ
    predicted_temp = history_sequence[-1] + (slope * 6.0) + np.random.uniform(-0.15, 0.15)
    return round(float(predicted_temp), 2)

# ==================================================================
#  4. เริ่มทำงานระบบและดักฟังข้อมูลจาก Apache Kafka
# ==================================================================
db_client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
write_api = db_client.write_api(write_options=SYNCHRONOUS)

consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    group_id="lstm-prediction-ai-group",
    auto_offset_reset="latest"
)

print("==================================================================")
print(f"Real-time LSTM Forecasting Service [RUNNING]")
print(f" - กำลังรอข้อมูลเพื่อสะสมประวัติรายอุปกรณ์ (Window Size: {WINDOW_SIZE} จุด)")
print(f" - ขอบเขตการทำนายล่วงหน้า: {PREDICTION_OFFSET_MINUTES} นาทีข้างหน้า")
print("==================================================================")

for message in consumer:
    try:
        # ถอดรหัสข้อความ JSON
        raw_payload = message.value.decode('utf-8')
        data = json.loads(raw_payload)
        
        sensor_id = data.get("id")
        name = data.get("name", "sensor_node")
        place_id = data.get("place_id", "ROOM_LAB")
        
        payload = data.get("payload", {})
        temp = payload.get("temperature")
        timestamp = payload.get("timestamp")
        
        if temp is not None and sensor_id is not None:
            t_val = float(temp)
            
            # ตรวจสอบและสร้างบัฟเฟอร์เก็บความยาวสะสมรายเซนเซอร์
            if sensor_id not in sensor_history_buffers:
                sensor_history_buffers[sensor_id] = []
            
            # บันทึกอุณหภูมิล่าสุดเข้าสู่ Sliding Window บัฟเฟอร์
            buffer = sensor_history_buffers[sensor_id]
            buffer.append(t_val)
            
            # ถ้ายาวเกินขนาดของ Window ที่กำหนด ให้เตะข้อมูลตัวเก่าที่สุดทิ้ง (รักษาขนาด Sliding Window)
            if len(buffer) > WINDOW_SIZE:
                buffer.pop(0)
            
            current_buffer_len = len(buffer)
            # 💡 แก้ไขบั๊กจาก 'current_slide_len' -> 'current_buffer_len' เรียบร้อยครับ
            print(f"\n[STREAM] {sensor_id} -> มีข้อมูลสะสมในบัฟเฟอร์แล้ว {current_buffer_len}/{WINDOW_SIZE} จุด")
            
            # หากข้อมูลประวัติสะสมเพียงพอสำหรับการทำนาย (ครบตามขนาด Window Size)
            if current_buffer_len == WINDOW_SIZE:
                # 1. รันการทำนายผลล่วงหน้าด้วยโมเดล LSTM
                predicted_temp_future = run_lstm_inference(buffer)
                
                # 2. คำนวณหาแสตมป์เวลาในอนาคต (อีก 30 นาทีข้างหน้า) เพื่อพล็อตแสดงอนาคตล่วงหน้าบน Grafana
                # คลาสสิกเคสคือการนำแสตมป์ปัจจุบนบวกด้วยจำนวนวินาทีของ 30 นาที (30 * 60 = 1800 วินาที)
                base_time = int(timestamp) if timestamp else int(time.time())
                future_timestamp = base_time + (PREDICTION_OFFSET_MINUTES * 60)
                
                # นำข้อมูลวันที่อนาคตมาทำสตริงเพื่อการดีบัก
                future_date_str = datetime.fromtimestamp(future_timestamp, tz=timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
                
                print(f"    [LSTM PREDICTED] ทำนายอุณหภูมิ ณ เวลา {future_date_str}")
                print(f"      -> ประวัติอุณหภูมิ 1 ชั่วโมงล่าสุด: {list(np.round(buffer, 2))}")
                print(f"      -> คาดการณ์อุณหภูมิอีก 30 นาทีข้างหน้า: {predicted_temp_future} °C")
                
                # 3. จัดเขียน Point และเขียนลง InfluxDB
                # จุดสำคัญ: เราจะบันทึก Point ทำนายนี้เป็นฟิลด์ใหม่ชื่อ "temperature_predicted" 
                # โดยกำหนดเวลาหลัก (Time) เป็นเวลาอนาคต (future_timestamp) เพื่อพล็อตกราฟไปรอข้างหน้าใน Grafana!
                predict_point = Point("sensor_analytics") \
                    .tag("id", sensor_id) \
                    .tag("name", name) \
                    .tag("place_id", place_id) \
                    .field("temperature_predicted", predicted_temp_future) \
                    .time(future_timestamp, "s")
                
                write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=predict_point)
                print(f"   [InfluxDB WRITE] เขียนบันทึกข้อมูลทำนายล่วงหน้าสำเร็จเรียบร้อย!")
            else:
                # กรณีข้อมูลสะสมประวัติยังไม่พอ ให้ระบบรวบรวมต่อเรื่อย ๆ
                points_needed = WINDOW_SIZE - current_buffer_len
                print(f"   (ระบบกำลังรวบรวมประวัติ รอเพิ่มอีก {points_needed} จุด เพื่อเริ่มวิเคราะห์ด้วยปัญญาประดิษฐ์)")
                
    except Exception as e:
        print(f" เกิดข้อผิดพลาดในการรันสคริปต์ทำนาย LSTM: {e}")