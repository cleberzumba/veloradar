import json
from datetime import datetime, timezone

import paho.mqtt.client as mqtt
import serial

SERIAL_PORT = "COM3"
BAUD_RATE = 9600
OUTPUT_FILE = "events.jsonl"

MQTT_HOST = "localhost"
MQTT_PORT = 1883
MQTT_TOPIC = "veloradar/events"


def main():
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="veloradar-serial-bridge")
    client.connect(MQTT_HOST, MQTT_PORT)
    client.loop_start()
    print(f"Connected to MQTT broker at {MQTT_HOST}:{MQTT_PORT}")

    try:
        with serial.Serial(SERIAL_PORT, BAUD_RATE, timeout=1) as arduino, \
             open(OUTPUT_FILE, "a", encoding="utf-8") as output:
            print(f"Connected to {SERIAL_PORT}. Publishing to '{MQTT_TOPIC}'...")

            while True:
                line = arduino.readline().decode("utf-8", errors="ignore").strip()
                if not line:
                    continue

                try:
                    event = json.loads(line)
                except json.JSONDecodeError:
                    print(f"Ignored (not JSON): {line}")
                    continue

                event["received_at"] = datetime.now(timezone.utc).isoformat()
                payload = json.dumps(event)

                output.write(payload + "\n")
                output.flush()

                client.publish(MQTT_TOPIC, payload, qos=1)
                print(f"Published: {payload}")
    finally:
        client.loop_stop()
        client.disconnect()


if __name__ == "__main__":
    main()