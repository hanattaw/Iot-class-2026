# Install telegraf
from https://www.influxdata.com/downloads/#telegraf

# influxdata-archive.key GPG fingerprint:
#   Primary key fingerprint: 24C9 75CB A61A 024E E1B6  3178 7C3D 5715 9FC2 F927
#   Subkey fingerprint:      9D53 9D90 D332 8DC7 D6C8  D3B9 D8FF 8E1F 7DF8 B07E
wget -q https://repos.influxdata.com/influxdata-archive.key
gpg --show-keys --with-fingerprint --with-colons ./influxdata-archive.key 2>&1 | grep -q '^fpr:\+24C975CBA61A024EE1B631787C3D57159FC2F927:$' && cat influxdata-archive.key | gpg --dearmor | sudo tee /etc/apt/trusted.gpg.d/influxdata-archive.gpg > /dev/null
echo 'deb [signed-by=/etc/apt/trusted.gpg.d/influxdata-archive.gpg] https://repos.influxdata.com/debian stable main' | sudo tee /etc/apt/sources.list.d/influxdata.list

# 2. อัปเดตแพ็กเกจระบบ
sudo apt-get update

# 3. และสั่งติดตั้ง Telegraf
sudo apt-get install telegraf

# สำรองไฟล์เก่าเก็บไว้ก่อน
sudo mv /etc/telegraf/telegraf.conf /etc/telegraf/telegraf.conf.bak

# เปิดไฟล์ใหม่ขึ้นมาแก้ไข
sudo nano /etc/telegraf/telegraf.conf

# เปิด ระบบ และทดสอบดู สนเ
sudo systemctl start telegraf
sudo systemctl enable telegraf
sudo systemctl status telegraf

# ดู log
sudo journalctl -u telegraf -n 20 -f

```conf
# Telegraf Configuration - Edge Gateway Role
# ทำหน้าที่: UDP Socket Listener -> Basic Stats Aggregator -> Template Formatter -> MQTT Publisher

[agent]
  interval = "5s"
  round_interval = true
  metric_batch_size = 1000
  metric_buffer_limit = 10000
  collection_jitter = "0s"
  flush_interval = "5s"
  flush_jitter = "0s"
  precision = "1s"
  hostname = "telegraf-gateway"
  omit_hostname = true

# ==================================================================
# 1. INPUT: เปิดรับข้อมูลจาก UDP
# ==================================================================
[[inputs.socket_listener]]
  service_address = "udp://:5005"
  data_format = "json_v2"

  [[inputs.socket_listener.json_v2]]
    [[inputs.socket_listener.json_v2.object]]
      path = "@this"
      tags = ["id", "name", "place_id"]
      disable_prepend_keys = true

    [[inputs.socket_listener.json_v2.field]]
      path = "payload.temperature"
      rename = "temperature"
      type = "float"

    [[inputs.socket_listener.json_v2.field]]
      path = "payload.humidity"
      rename = "humidity"
      type = "float"

    [[inputs.socket_listener.json_v2.field]]
      path = "payload.pressure"
      rename = "pressure"
      type = "float"

# ==================================================================
# 2. AGGREGATOR: หาค่าเฉลี่ย (คำนวณทุกๆ 60 วินาที)
# ==================================================================
[[aggregators.basicstats]]
  period = "60s"
  drop_original = true
  stats = ["mean"]
  name_suffix = "" # คงชื่อเดิมไว้ เช่น "temperature" ไม่ใช่ "temperature_mean"

# ==================================================================
# 3. PROCESSOR: ตัดคำเอา client_id ออกมาจาก id (แยกบล็อกออกมาให้ถูกต้อง)
# ==================================================================
[[processors.regex]]
  [[processors.regex.tags]]
    key = "id"
    pattern = '^ID_(?P<client_id>\d+)'
    replacement = "${client_id}"
    result_key = "client_id"

# ==================================================================
# 4. OUTPUT: ส่งข้อมูลไปยัง MQTT Broker
# ==================================================================
[[outputs.mqtt]]
  servers = ["tcp://172.16.2.117:1883"]
  
  # ดึงค่าจาก Tag "client_id" ที่ถูกสร้างจากขั้นตอน processors.regex มาใช้งาน
  topic = 'v1/{{ .Tag "client_id" }}'
  
  qos = 1
  data_format = "template"
  
  # ใช้รูปแบบ Multiline String (''') เพื่อให้เขียน JSON สวยงามและตัดปัญหาเรื่อง Syntax Error จากฟันหนู (\")
  template = '''
{
  "id": "{{.Tag "id"}}",
  "name": "{{.Tag "name"}}",
  "place_id": "{{.Tag "place_id"}}",
  "payload": {
    "temperature": {{.Field "temperature_mean"}},
    "humidity": {{.Field "humidity_mean"}},
    "pressure": {{.Field "pressure_mean"}},
    "timestamp": {{.Time.Unix}},
    "date": "{{.Time.Format "2006-01-02T15:04:05Z07:00"}}"
  }
}
'''
  ```

  sudo systemctl restart telegraf

