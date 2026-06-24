/*
* iot-class.ino
* iot-class from ESP32 Cucumber RIS with temperature, humidity, pressure, acceleration, angular_velocity, battery_voltage_mv (generate random)
* function: 
* 1. Connect to Wifi
* 2. Connect to MQTT
* 3. Publish data to MQTT for each sensor data
*/

#include <Wire.h>
#include <Adafruit_BMP280.h>
#include <Adafruit_MPU6050.h>
#include <SensirionI2cSht4x.h>
#include <PubSubClient.h>
#include <WiFi.h>
#include <ArduinoJson.h>
#include <Adafruit_NeoPixel.h>

// Sensirion SHt4x
#define SDA_PIN 41
#define SCL_PIN 40
#define CLOCK_FEQ 100000
#define LED_BUILTIN 2 // LED Pin
SensirionI2cSht4x sht4x;

// NeoPixel
#define LEDPIN 18
#define NUMPIXELS 1
Adafruit_NeoPixel pixels(NUMPIXELS, LEDPIN, NEO_RGB + NEO_KHZ800);

// กำหนดสถานะตามทฤษฎี IoT ด้วย enum
enum SystemState {
  STATE_SENSING,      // กำลังอ่านค่าเซนเซอร์ -> สีฟ้า (BLUE)
  STATE_TX_SUCCESS,   // ส่งข้อมูลสำเร็จ -> สีเขียว (GREEN)
  STATE_NETWORK_WAIT, // เครือข่ายกำลังเชื่อมต่อ/พยายามต่อใหม่ -> สีเหลือง/ส้ม (ORANGE)
  STATE_ERROR         // เกิดข้อผิดพลาดของฮาร์ดแวร์/ซอฟต์แวร์ -> สีแดง (RED)
};

SystemState currentState = STATE_NETWORK_WAIT; // เริ่มต้นที่สถานะรอเน็ต

#define MQTT_BROKER   "172.16.46.53"
#define MQTT_PORT     1883
#define MQTT_USERNAME ""
#define MQTT_PASSWORD ""
#define MQTT_NAME     "Cucumber_RIS_Node"

#ifdef NO_ERROR
#undef NO_ERROR
#endif
#define NO_ERROR 0

//*** UPDATE THESE SETTINGS
const char* ssid         = "Net_FDT";
const char* password     = "Cdti2358";

// BPM280
Adafruit_BMP280 bmp;

// MPU6050
Adafruit_MPU6050 mpu;

static char errorMessage[64];
static int16_t error;

// Wifi
WiFiClient client;
// MQTT
PubSubClient mqtt(client);

// ตัวแปรควบคุมเวลา Non-blocking (Timers)
unsigned long prev_sensor_millis = 0;
unsigned long prev_blink_millis = 0;
bool ledToggleState = false; // สำหรับทำไฟกระพริบ

void setupHardware() {
    // กำหนด Wire.begin เพียงครั้งเดียวเพื่อป้องกัน Bus ชนกัน
    Wire.begin(SDA_PIN, SCL_PIN, CLOCK_FEQ);
    
    // Pixel setup
    pixels.begin();
    pixels.setBrightness(40); // กำหนดความสว่างที่เหมาะสม
  
    // prepare BMP280 sensor
    if (bmp.begin(0x76)) {
      Serial.println("BMP280 sensor ready");
    } else {
      Serial.println("BMP280 sensor fail!");
      currentState = STATE_ERROR;
    }

    // Sensirion setup
    sht4x.begin(Wire, SHT40_I2C_ADDR_44);
    sht4x.softReset();
    delay(10);
    
    uint32_t serialNumber = 0;
    error = sht4x.serialNumber(serialNumber);
    if (error != NO_ERROR) {
      Serial.print("Error trying to execute serialNumber(): ");
      errorToString(error, errorMessage, sizeof errorMessage);
      Serial.println(errorMessage);
      currentState = STATE_ERROR;
      return;
    }

    Serial.print("serialNumber: ");
    Serial.println(serialNumber);

    // prepare MPU6050 sensor
    if (mpu.begin()) { 
       Serial.println("MPU6050 sensor ready");
    } else {
       Serial.println("MPU6050 sensor fail!");
       currentState = STATE_ERROR;
    }

    pinMode(LED_BUILTIN, OUTPUT);
    digitalWrite(LED_BUILTIN, HIGH);
}

// --- เพิ่มส่วนนี้ไว้ด้านบน ---
IPAddress local_IP(172, 16, 46, 54);  // ตั้ง IP ให้ไม่ซ้ำกับตัว UDP
IPAddress gateway(172, 16, 46, 254);
IPAddress subnet(255, 255, 255, 0);
IPAddress primaryDNS(8, 8, 8, 8);

void setup() {
  Serial.begin(115200);

  setupHardware();
  Serial.println("Starting");
  randomSeed(analogRead(0));

  // ----------------------------------------------------
  // เพิ่มการตั้งค่า Static IP ตรงนี้ (ก่อน WiFi.begin)
  // ----------------------------------------------------
  if (!WiFi.config(local_IP, gateway, subnet, primaryDNS)) {
    Serial.println("STA Failed to configure Static IP");
  }
  
  // Initiate Wi-Fi connection setup
  WiFi.begin(ssid, password);
  Serial.print("\r\nConnecting to ");
  Serial.print(ssid); Serial.print(" ...");
  
  // จุดนี้ยังปล่อยให้เป็นแบบ Blocking ในช่วงแรกเพื่อให้บอร์ดพร้อมใช้งานก่อนเข้า loop()
  while (WiFi.status() != WL_CONNECTED) {
    delay(500);
    Serial.print(".");
    pixels.setPixelColor(0, pixels.Color(30, 20, 0)); // เปิดไฟสีเหลืองกระพริบตอนต่อไวไฟ
    pixels.show();
    delay(100);
    pixels.setPixelColor(0, pixels.Color(0, 0, 0));
    pixels.show();
  }
  Serial.print(" Connected! IP address: ");
  Serial.println(WiFi.localIP());

  mqtt.setServer(MQTT_BROKER, MQTT_PORT);
  currentState = STATE_SENSING; // เมื่อพร้อมแล้ว เปลี่ยนเป็นสถานะกำลังเริ่มต้นทำงาน
}

void reconnect() {
  // บล็อกสั้นๆ เฉพาะในกรณีที่ MQTT หลุด เพื่อพยายามซ่อมแซมการเชื่อมต่อ
  if (!mqtt.connected()) {
    Serial.print("Attempting MQTT connection...");
    if (mqtt.connect(MQTT_NAME)){
      Serial.println("connected");
      currentState = STATE_SENSING;
    } else {
      Serial.print("failed, rc=");
      Serial.print(mqtt.state());
      Serial.println(" will try again in next task cycle");
      currentState = STATE_NETWORK_WAIT; // เปลี่ยนไปสถานะหลุดการเชื่อมต่อเพื่อแสดงไฟสีส้ม
    }
  }
}

void loop() {
  unsigned long currentTime = millis(); // ตรวจสอบเวลาปัจจุบันเสมอที่ต้นลูป
  
  sensors_event_t temp;
  sensors_event_t a, g;

  char json[] = R"raw(
        {
            "id": "99999999",
            "name": "iot_sensor_99",
            "place_id": "32347983",
            "payload": {
                "temperature": -1,
                "humidity": 41,
                "pressure": 1023
            }
        })raw";

  DynamicJsonDocument doc(1024);
  deserializeJson(doc, json);

  // ตรวจสอบการเชื่อมต่อ MQTT (Non-blocking check)
  if(!mqtt.connected()) {
    // แทนที่จะปล่อยให้ลูปค้างตลอดเวลา ให้เรียกตรวจเฉพาะจังหวะเครือข่ายหลุด
    reconnect(); 
  }
  mqtt.loop(); // ต้องทำงานตลอดเวลา ห้ามมีคำสั่ง delay() ขวางเด็ดขาด

  // ----------------------------------------------------
  // TASK 1: จัดการประมวลผลเซนเซอร์และส่งข้อมูล (ทุก ๆ 5 วินาที) - Non-blocking
  // ----------------------------------------------------
  if ((currentTime - prev_sensor_millis) > 5000) {
    prev_sensor_millis = currentTime;
    
    if (currentState != STATE_ERROR) {
      currentState = STATE_SENSING; // สลับเป็นสีฟ้าชั่วคราวขณะเข้าทำงาน
    }

    // 1. อ่านค่าจาก BMP280
    float pressure = bmp.readPressure();

    // 2. อ่านค่าจาก SHT41
    uint16_t sht_error;
    char sht_errorMessage[256];
    float temperature = 0.0;
    float humidity = 0.0;
    
    sht_error = sht4x.measureHighPrecision(temperature, humidity);
    if (sht_error) {
      Serial.print("Error trying to execute measureHighPrecision(): ");
      errorToString(sht_error, sht_errorMessage, 256);
      Serial.println(sht_errorMessage);
      currentState = STATE_ERROR;
    }

    // 3. อ่านค่าจาก MPU6050
    mpu.getEvent(&a, &g, &temp);
    float ax = a.acceleration.x;
    float ay = a.acceleration.y;
    float az = a.acceleration.z;
    float gx = g.gyro.x;
    float gy = g.gyro.y;
    float gz = g.gyro.z;

    // 4. สุ่มโวลต์แบตเตอรี่
    unsigned int b = random(2900, 3000);
  
    // 5. ประกอบและส่งข้อมูล JSON Payloadไปยัง Gateway
    JsonObject payload = doc["payload"];
    payload["temperature"] = temperature;
    payload["humidity"] = humidity;
    payload["pressure"] = pressure;

    String jsonPayload;
    serializeJson(doc, jsonPayload);
    
    if (mqtt.connected()) {
      mqtt.publish("iot-frames-model", jsonPayload.c_str());
      Serial.println("Published sensor data to MQTT");
      Serial.println(jsonPayload);
      
      if (currentState != STATE_ERROR) {
        currentState = STATE_TX_SUCCESS; // หากส่งสำเร็จและเครื่องไม่มีปัญหา ให้ปรับเป็นสีเขียว
      }
    } else {
      if (currentState != STATE_ERROR) {
        currentState = STATE_NETWORK_WAIT; // ถ้าเน็ตหลุดปรับเป็นสีส้ม/เหลือง
      }
    }
  }

  // ----------------------------------------------------
  // TASK 2: ควบคุมและกระพริบไฟสถานะ NeoPixel (ทุกๆ 500ms) - Non-blocking
  // ----------------------------------------------------
  if ((currentTime - prev_blink_millis) > 500) {
    prev_blink_millis = currentTime;
    ledToggleState = !ledToggleState; // สลับสัญญานเพื่อทำการกระพริบ

    if (ledToggleState) {
      // จังหวะไฟสว่าง: ตรวจสอบสถานะและแสดงสีตามทฤษฎีที่สอน
      switch (currentState) {
        case STATE_SENSING:
          pixels.setPixelColor(0, pixels.Color(0, 0, 40));    // สีฟ้า (BLUE)
          break;
        case STATE_TX_SUCCESS:
          pixels.setPixelColor(0, pixels.Color(0, 40, 0));    // สีเขียว (GREEN)
          break;
        case STATE_NETWORK_WAIT:
          pixels.setPixelColor(0, pixels.Color(40, 25, 0));   // สีส้ม/เหลือง (ORANGE/YELLOW)
          break;
        case STATE_ERROR:
          pixels.setPixelColor(0, pixels.Color(40, 0, 0));    // สีแดง (RED)
          break;
      }
    } else {
      // จังหวะไฟดับ เพื่อให้เกิดการกระพริบสังเกตง่าย
      pixels.setPixelColor(0, pixels.Color(0, 0, 0));
    }
    pixels.show(); // สั่งให้ฮาร์ดแวร์แสดงผลไฟ
  }

}