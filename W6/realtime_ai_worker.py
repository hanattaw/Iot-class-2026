import json
import math
import time
from datetime import datetime, timezone
from kafka import KafkaConsumer
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
from sklearn.ensemble import IsolationForest
import numpy as np

# ==================================================================
#  1. ส่วนกำหนดค่าการเชื่อมต่อ (Configuration)
# ==================================================================
# ที่อยู่ของระบบ Apache Kafka (ดึงจากเครือข่าย Docker หรือ Localhost)
KAFKA_BOOTSTRAP_SERVERS = ["localhost:9092"] 
KAFKA_TOPIC = "university.sensors.telemetry"

# ที่อยู่และสิทธิ์การเข้าถึงของฐานข้อมูล InfluxDB v2
INFLUX_URL = "http://localhost:8086"
INFLUX_TOKEN = "mytoken"               # อ้างอิงตามค่าเริ่มต้นใน Docker Compose
INFLUX_ORG = "my-org"
INFLUX_BUCKET = "iot_data_stream"              # บักเก็ตสำหรับเก็บข้อมูลวิเคราะห์

# ==================================================================
#  2. เริ่มต้นระบบประมวลผลโมเดล Machine Learning (Isolation Forest)
# ==================================================================
# โมเดล Isolation Forest สำหรับตรวจจับความผิดปกติของอุณหภูมิ (Anomaly Detection)
# กำหนดค่า contamination เป็น 0.05 (คาดการณ์ว่าอาจมีข้อมูลผิดปกติเกิดขึ้นประมาณ 5%)
ml_model = IsolationForest(contamination=0.05, random_state=42)

# จำลองกลุ่มข้อมูลอุณหภูมิปกติ (24°C ถึง 28°C) เพื่อให้โมเดลเรียนรู้โครงสร้างเริ่มต้น (Fit)
# ในสถานการณ์จริงเราสามารถโหลดไฟล์โมเดลที่เทรนสำเร็จแล้ว (.pkl) ขึ้นมาใช้ได้ทันที
print("[AI Model] กำลังเริ่มเทรนโมเดลตรวจจับความผิดปกติเบื้องต้น...")
baseline_data = np.random.uniform(24.0, 28.0, (200, 1))
ml_model.fit(baseline_data)
print("[AI Model] เทรนโมเดลเริ่มต้นเสร็จสิ้น! พร้อมวิเคราะห์สตรีมข้อมูลเรียลไทม์")

# ==================================================================
#  3. ฟังก์ชันคำนวณ Soft Sensors เชิงฟิสิกส์และอุตุนิยมวิทยา
# ==================================================================
def calculate_dew_point(temp, humid):
    """ สูตรคำนวณจุดน้ำค้าง (Dew Point) ด้วยสมการ Magnus-Tetens """
    a = 17.625
    b = 243.04
    try:
        if humid <= 0:
            return 0.0
        gamma = ((a * temp) / (b + temp)) + math.log(humid / 100.0)
        dew_point = (b * gamma) / (a - gamma)
        return round(dew_point, 2)
    except Exception as e:
        print(f" เกิดข้อผิดพลาดในการคำนวณ Dew Point: {e}")
        return 0.0

def calculate_vpd(temp, humid):
    """ สูตรคำนวณค่าต่างของแรงดันไอ (Vapor Pressure Deficit) หน่วย kPa """
    try:
        if humid <= 0:
            return 0.0
        # 1. หาแรงดันไอน้ำอิ่มตัว (Saturated Vapor Pressure)
        vp_sat = 0.61078 * math.exp((17.27 * temp) / (temp + 237.3))
        # 2. หาแรงดันไอน้ำจริง (Actual Vapor Pressure)
        vp_act = vp_sat * (humid / 100.0)
        # 3. คำนวณหาผลต่างแรงดันไอ
        vpd = vp_sat - vp_act
        return round(vpd, 3)
    except Exception as e:
        print(f" เกิดข้อผิดพลาดในการคำนวณ VPD: {e}")
        return 0.0

def calculate_altitude(pressure):
    """ สูตรคำนวณระดับความสูงสัมพัทธ์ (Altitude) เหนือระดับน้ำทะเลอ้างอิงจากความดันบรรยากาศ """
    # กำหนดความกดอากาศมาตรฐานที่ระดับน้ำทะเลปกติ (101325 Pa)
    p0 = 101325.0
    try:
        if pressure <= 0:
            return 0.0
        altitude = 44330.0 * (1.0 - math.pow(pressure / p0, 1.0 / 5.255))
        return round(altitude, 2)
    except Exception as e:
        print(f" เกิดข้อผิดพลาดในการคำนวณ Altitude: {e}")
        return 0.0

# ==================================================================
#  4. เริ่มต้นเชื่อมต่อบริการปลายทาง (Services Init)
# ==================================================================
# เชื่อมต่อ InfluxDB Client
try:
    db_client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
    write_api = db_client.write_api(write_options=SYNCHRONOUS)
    print("[InfluxDB] เชื่อมต่อระบบเขียนข้อมูลสำเร็จ")
except Exception as e:
    print(f" [InfluxDB Error] ไม่สามารถเชื่อมต่อได้: {e}")
    exit(1)

# เชื่อมต่อคอยฟังข้อมูลจาก Apache Kafka
try:
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id="ai-realtime-processor-group",
        auto_offset_reset="latest"
    )
    print(f"[Kafka] รอรับข้อมูลจาก Topic: '{KAFKA_TOPIC}' สำเร็จ")
except Exception as e:
    print(f" [Kafka Error] ไม่สามารถเชื่อมต่อ Broker ได้: {e}")
    exit(1)

# ==================================================================
#  5. ประมวลผลลูปข้อมูลเรียลไทม์แบบไร้รอยต่อ (Event Loop)
# ==================================================================
print("\n==================================================================")
print("  Real-time AI & Soft Sensor Service [RUNNING]")
print(" - กำลังรอประมวลผลข้อมูลสตรีมมิ่งสดจากเครือข่ายคาฟก้า...")
print("==================================================================")

for message in consumer:
    try:
        # ถอดรหัส JSON จาก Kafka Event
        raw_data = message.value.decode('utf-8')
        data = json.loads(raw_data)
        
        sensor_id = data.get("id")
        name = data.get("name", "sensor_node")
        place_id = data.get("place_id", "ROOM_LAB")
        
        payload = data.get("payload", {})
        temp = payload.get("temperature")
        humid = payload.get("humidity")
        press = payload.get("pressure")
        timestamp = payload.get("timestamp")
        
        if temp is not None and humid is not None and press is not None:
            # แปลงประเภทข้อมูลให้ถูกต้อง
            t_val = float(temp)
            rh_val = float(humid)
            p_val = float(press)
            
            # --- ขั้นตอนที่ 1: ประมวลผลคำนวณ Soft Sensors ---
            dew_point = calculate_dew_point(t_val, rh_val)
            vpd = calculate_vpd(t_val, rh_val)
            altitude = calculate_altitude(p_val)
            
            # --- ขั้นตอนที่ 2: ป้อนเข้าโมเดล Machine Learning เพื่อวิเคราะห์ Anomaly ---
            # ป้อนค่าอุณหภูมิเข้าไปให้โมเดลประเมิน
            test_data = np.array([[t_val]])
            prediction = ml_model.predict(test_data)[0]
            # ผลลัพธ์: 1 = ปกติ, -1 = ผิดปกติ (เราจะแปลงเป็น 0 และ 1 เพื่อพล็อตกราฟง่ายขึ้น)
            anomaly_status = 0 if prediction == 1 else 1
            
            # แสดงสถิติการตรวจพบบนหน้าจอคอนโซล
            print(f"\n[EVENT] ID: {sensor_id} | T: {t_val}°C, RH: {rh_val}%, P: {p_val} Pa")
            print(f"   └─► [COMPUTED] DewPoint: {dew_point}°C | VPD: {vpd} kPa | Altitude: {altitude} m")
            
            if anomaly_status == 1:
                print(f"   └─► [AI STATUS]  ตรวจพบอุณหภูมิผิดปกติในพื้นที่! (Anomaly Status = 1)")
            else:
                print(f"   └─► [AI STATUS]  สภาพแวดล้อมปกติ")

            # --- ขั้นตอนที่ 3: บันทึกข้อมูลและค่า Soft Sensors/AI ลงสู่ InfluxDB ---
            point = Point("sensor_analytics") \
                .tag("id", sensor_id) \
                .tag("name", name) \
                .tag("place_id", place_id) \
                .field("temperature", t_val) \
                .field("humidity", rh_val) \
                .field("pressure", p_val) \
                .field("dew_point", float(dew_point)) \
                .field("vpd", float(vpd)) \
                .field("altitude", float(altitude)) \
                .field("ai_anomaly", int(anomaly_status))
            
            # ตรวจสอบและยัดแกนเวลา
            if timestamp:
                point.time(int(timestamp), "s")
            else:
                point.time(int(time.time()), "s")
                
            # ส่งข้อมูลไปเขียนในบักเก็ต iot_data แบบเรียลไทม์
            write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=point)
            print(f"   └─► [InfluxDB] เขียนบันทึกข้อมูลวิเคราะห์ลง Bucket สำเร็จ!")
            
    except Exception as e:
        print(f" เกิดข้อผิดพลาดในขั้นตอนประมวลผล: {e}")