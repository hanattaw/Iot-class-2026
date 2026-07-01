# UDP Bridge

MQTT_BROKER = "172.16.2.117"  # ไอพีของ MQTT Broker (หรือเครื่อง Gateway เอง)
MQTT_PORT = 1883
MQTT_TOPIC = "v1/68123456789"
MQTT_CLIENT_ID = "GW_68123456789"

```bash
docker run --rm eclipse-mosquitto mosquitto_sub -h 172.16.2.117 -t "v1_68123456789" -v
```
