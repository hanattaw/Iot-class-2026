import asyncio
import paho.mqtt.client as mqtt
import json

# === กำหนดค่าเชื่อมต่อ MQTT Broker ===
MQTT_BROKER = "172.16.2.117"  # ไอพีของ MQTT Broker (หรือเครื่อง Gateway เอง)
MQTT_PORT = 1883
MQTT_TOPIC = "v1/68123456789"
MQTT_CLIENT_ID = "GW_68123456789"

# สร้าง Client สำหรับเชื่อมต่อ MQTT (รองรับ Paho-MQTT v2 API)
try:
    mqtt_client = mqtt.Client(client_id=MQTT_CLIENT_ID, callback_api_version=mqtt.CallbackAPIVersion.VERSION2)
except AttributeError:
    # รองรับกรณีใช้ Paho-MQTT v1.x รุ่นเก่า
    mqtt_client = mqtt.Client(client_id=MQTT_CLIENT_ID)

class AsyncUDPReceiverProtocol(asyncio.DatagramProtocol):
    def connection_made(self, transport):
        self.transport = transport
        print("==================================================")
        print(" UDP to MQTT Gateway เริ่มทำงานแล้ว...")
        print(" กำลังรอรับข้อมูล UDP ที่พอร์ต 5005...")
        print("==================================================")

    def datagram_received(self, data, addr):
        # ฟังก์ชันนี้จะถูกเรียกทำงานอัตโนมัติเมื่อได้รับ UDP Packet
        try:
            # แปลงบิตข้อมูล (Bytes) เป็นข้อความ String (UTF-8)
            message = data.decode('utf-8')
            print(f"\n[UDP IN] ได้รับข้อมูลจากอุปกรณ์ {addr}:")
            print(f" -> Payload: {message}")

            # ตรวจสอบว่าข้อมูลที่ได้เป็น JSON หรือไม่
            try:
                parsed_json = json.loads(message)
                # ส่งข้อมูลแบบ JSON ไปยัง MQTT Broker
                mqtt_client.publish(MQTT_TOPIC, json.dumps(parsed_json), qos=1)
                print(f"[MQTT OUT] ส่งข้อมูลไปยัง Topic '{MQTT_TOPIC}' เรียบร้อยแล้ว (QoS 1)")
            except json.JSONDecodeError:
                # กรณีที่บอร์ดส่งมาเป็นข้อความธรรมดา ให้ส่งขึ้น MQTT ตรงๆ
                mqtt_client.publish(MQTT_TOPIC, message, qos=0)
                print(f"[MQTT OUT] ส่งข้อความธรรมดาไปยัง Topic '{MQTT_TOPIC}' (QoS 0)")

        except Exception as e:
            print(f"เกิดข้อผิดพลาดในการประมวลผลข้อมูล: {e}")

async def main():
    # 1. เริ่มทำการเชื่อมต่อกับ MQTT Broker
    print(f"กำลังเชื่อมต่อกับ MQTT Broker ที่ {MQTT_BROKER}:{MQTT_PORT}...")
    try:
        mqtt_client.connect(MQTT_BROKER, MQTT_PORT, keepalive=60)
        # สตาร์ทลูปเบื้องหลังของ MQTT เพื่อให้ทำงานร่วมกับ Asyncio ได้อย่างราบรื่น
        mqtt_client.loop_start()
        print("เชื่อมต่อ MQTT Broker สำเร็จ!")
    except Exception as e:
        print(f"ไม่สามารถเชื่อมต่อ MQTT Broker ได้: {e}")
        print("โปรดตรวจสอบ IP ของ Broker หรือการเชื่อมต่อเครือข่าย")
        return

    # 2. ทำการเปิดพอร์ตรับข้อมูล UDP (Port 5005)
    loop = asyncio.get_running_loop()
    transport, protocol = await loop.create_datagram_endpoint(
        lambda: AsyncUDPReceiverProtocol(),
        local_addr=('0.0.0.0', 5005)
    )

    try:
        # ลูปให้โปรแกรมทำงานอย่างต่อเนื่องแบบ Non-blocking
        while True:
            await asyncio.sleep(3600)  # พักการทำงานเป็นคาบเวลาสั้นๆ ใน Loop
    except asyncio.CancelledError:
        print("\nกำลังยกเลิกการทำงาน...")
    finally:
        # ทำความสะอาดการเชื่อมต่อเมื่อปิดโปรแกรม
        transport.close()
        mqtt_client.loop_stop()
        mqtt_client.disconnect()
        print("ปิดการทำงานของ Gateway เรียบร้อยแล้ว")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nปิดโปรแกรมสำเร็จด้วยคีย์บอร์ด")