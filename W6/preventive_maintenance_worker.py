import json
import math
import time
import numpy as np
from kafka import KafkaConsumer
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
from sklearn.ensemble import RandomForestRegressor

# ==================================================================
#  1. ส่วนกำหนดค่าเชื่อมต่อ (Configurations)
# ==================================================================
KAFKA_BOOTSTRAP_SERVERS = ["localhost:9092"]
KAFKA_TOPIC = "university.sensors.telemetry"

INFLUX_URL = "http://localhost:8086"
INFLUX_TOKEN = "mytoken"
INFLUX_ORG = "my-org"
INFLUX_BUCKET = "iot_data_stream"

# ==================================================================
#  2. เริ่มต้นโมเดลบำรุงรักษาเชิงพยากรณ์ (ML Model Init)
# ==================================================================
# ใช้ Random Forest Regressor สำหรับทำนายค่าความสมบูรณ์พัดลม (Health Score: 100% คือสมบูรณ์แบบ, 0% คือชำรุด)
ml_model = RandomForestRegressor(n_estimators=50, random_state=42)

# จำลองตารางเรียนรู้เชิงอุตสาหกรรม (Training Simulation) เพื่อสร้างสมองกลเริ่มต้น
# Features: [Temperature, Humidity, VPD, Current_Draw (mA), Vibration_RMS (g)]
# Targets: Health_Score (0 - 100)
print("[AI Engine] กำลังสร้างตารางเรียนรู้เชิงวิศวกรรม (Feature Correlation)...")
X_train = np.array([
    [25.0, 40.0, 1.2, 120.0, 0.05],  # สภาวะปกติ พัดลมลื่นกินกระแสต่ำ สั่นสะเทือนน้อย
    [26.5, 45.0, 1.1, 122.0, 0.06],
    [28.0, 50.0, 0.9, 135.0, 0.12],  # เริ่มอุ่นขึ้น พัดลมขยับสั่นสะเทือนขึ้นเล็กน้อย
    [32.0, 60.0, 0.7, 160.0, 0.28],  # ตู้ร้อน ลูกปืนฝืด มอเตอร์กินกระแสเพิ่มขึ้นเพื่อรักษารอบ
    [35.5, 75.0, 0.4, 210.0, 0.65],  # วิกฤต! อุณหภูมิและความชื้นสะสมสูง ลูกปืนขัด สั่นสะเทือนรุนแรง กินกระแสพีคสุด
])
y_train = np.array([100.0, 95.0, 80.0, 50.0, 5.0]) # ค่าสุขภาพประเมินของฮาร์ดแวร์ (%)
ml_model.fit(X_train, y_train)
print("[AI Engine] โมเดลเรียนรู้สหสัมพันธ์สำเร็จ! พร้อมคอยเฝ้าระวังพฤติกรรมพัดลม")

# ==================================================================
#  3. ฟังก์ชันคำนวณดัชนีคัดสรร (Derived Soft Sensors)
# ==================================================================
def calculate_vpd(temp, humid):
    try:
        if humid <= 0: return 0.0
        vp_sat = 0.61078 * math.exp((17.27 * temp) / (temp + 237.3))
        vp_act = vp_sat * (humid / 100.0)
        return round(vp_sat - vp_act, 3)
    except:
        return 0.0

# ==================================================================
#  4. เริ่มรันระบบดักฟังและสั่งพยากรณ์เรียลไทม์
# ==================================================================
db_client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
write_api = db_client.write_api(write_options=SYNCHRONOUS)

consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    group_id="predictive-maintenance-ai-group",
    auto_offset_reset="latest"
)

print("\n==================================================================")
print("   Real-time Predictive Maintenance AI Service [RUNNING]")
print(" - เชื่อมโยงสถิติสภาพแวดล้อมและพฤติกรรมพัดลมเพื่อลดการพังเสียหาย")
print("==================================================================")

for message in consumer:
    try:
        raw_payload = message.value.decode('utf-8')
        data = json.loads(raw_payload)
        
        sensor_id = data.get("id")
        name = data.get("name", "sensor_node")
        place_id = data.get("place_id", "ROOM_LAB")
        
        payload = data.get("payload", {})
        temp = payload.get("temperature")
        humid = payload.get("humidity")
        
        #  ในพาร์ทการประยุกต์ใช้งานจริง บอร์ด ESP32 ของเด็กๆ จะส่งค่าพฤติกรรมทางฟิสิกส์ของพัดลมเพิ่มเติมเข้ามาใน JSON
        # หากเซนเซอร์หน้างานไม่มีการวัดจริง ตัวสคริปต์นี้จะทำหน้าที่สุ่มเลียนแบบความฝืดเชิงสหสัมพันธ์ (Simulated Correlation)
        fan_current = payload.get("fan_current")
        fan_vibration = payload.get("fan_vibration")
        
        if temp is not None and humid is not None:
            t_val = float(temp)
            rh_val = float(humid)
            vpd_val = calculate_vpd(t_val, rh_val)
            
            # ตรวจจับสร้างข้อมูลสหสัมพันธ์หากไม่มีอุปกรณ์กายภาพจริงหน้างาน (Safe Guard)
            if fan_current is None:
                # จำลองการกินกระแสไฟฟ้าของพัดลมตามความหนืดที่แปรผันผกผันกับค่าสภาพแวดล้อม
                fan_current = 120.0 + (t_val - 25.0) * 8.0 + np.random.uniform(-5.0, 5.0)
                fan_current = round(max(100.0, fan_current), 2)
            if fan_vibration is None:
                # จำลองแรงสั่นสะเทือนของลูกปืนพัดลมเมื่อเกิดความร้อนสะสม
                fan_vibration = 0.05 + (t_val - 25.0) * 0.05 + np.random.uniform(-0.02, 0.02)
                fan_vibration = round(max(0.01, fan_vibration), 3)

            # จัดเตรียมคุณสมบัติ (Features) นำเข้าสู่โมเดลปัญญาประดิษฐ์
            features = np.array([[t_val, rh_val, vpd_val, fan_current, fan_vibration]])
            
            # 1. ทำนายค่าดัชนีสุขภาพของพัดลม (Predicted Health Score: 0% ถึง 100%)
            health_score = ml_model.predict(features)[0]
            health_score = round(max(0.0, min(100.0, health_score)), 2)
            
            # 2. คำนวณทำนายอายุการใช้งานที่เหลืออยู่ (Remaining Useful Life: RUL) ในหน่วยชั่วโมง
            # แปลงอย่างง่ายเชิงวิศวกรรม: สุขภาพลดลง 1% เทียบเท่าโอกาสวิ่งงานรอดลดลง 5 ชั่วโมง
            predicted_rul_hours = round(health_score * 5.0, 1)
            
            # 3. ประเมินระดับความเร่งในการบำรุงรักษาพัดลมเพื่อจัดระดับส่งใบแจ้งซ่อม (Maintenance Priority)
            # 0 = ปกติ, 1 = เฝ้าระวัง (เตือนเปลี่ยน), 2 = วิกฤตพังเฉียบพลัน (ปิดตู้และสั่งช่างเปลี่ยนทันที)
            if health_score > 70.0:
                maintenance_status = 0  # สุขภาพดี
                status_desc = "✅ NORMAL (พัดลมทำงานปกติ)"
            elif health_score > 35.0:
                maintenance_status = 1  # เริ่มผิดปกติ ควรนัดตรวจสภาพ
                status_desc = "⚠️ WARNING (ควรวางคิวตรวจสภาพและหยอดน้ำมันแบริ่งพัดลม)"
            else:
                maintenance_status = 2  # ความร้อนวิกฤต มอเตอร์ฝืดรุนแรง ใกล้หยุดหมุน
                status_desc = "🚨 CRITICAL WARNING! (พัดลมใกล้หยุดหมุน สั่งการช่างเข้าเปลี่ยนใบพัดทันที)"
                
            print(f"\n[EVALUATED] Sensor: {sensor_id} | T: {t_val}°C, RH: {rh_val}%")
            print(f"   -> Fan Current Draw: {fan_current} mA | Fan Vibration: {fan_vibration} g")
            print(f"   -> [ML Prediction] Fan Health Index: {health_score} % | Estimated RUL: {predicted_rul_hours} Hours")
            print(f"   -> [Decision Service] Status: {status_desc}")
            
            # 4. จัดเขียน Point และเขียนข้อมูลทั้งหมดลง InfluxDB
            point = Point("fan_predictive_maintenance") \
                .tag("id", sensor_id) \
                .tag("name", name) \
                .tag("place_id", place_id) \
                .field("temperature", t_val) \
                .field("humidity", rh_val) \
                .field("vpd", vpd_val) \
                .field("fan_current", fan_current) \
                .field("fan_vibration", fan_vibration) \
                .field("fan_health_score", health_score) \
                .field("fan_rul_hours", predicted_rul_hours) \
                .field("maintenance_priority", int(maintenance_status))
            
            write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=point)
            print(f"   -> [InfluxDB WRITE] บันทึกผลการพยากรณ์และจัดลำดับใบแจ้งซ่อมสำเร็จ!")
            
    except Exception as e:
        print(f"❌ เกิดข้อผิดพลาดในการคำนวณ Predictive Maintenance: {e}")