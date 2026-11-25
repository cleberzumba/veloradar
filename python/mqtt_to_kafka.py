import paho.mqtt.client as mqtt
from confluent_kafka import Producer

MQTT_HOST = "localhost"
MQTT_PORT = 1883
MQTT_TOPIC = "veloradar/events"

KAFKA_BOOTSTRAP = "localhost:9092"
KAFKA_TOPIC = "veloradar-events"
KAFKA_KEY = "radar-01"

producer = Producer({
    "bootstrap.servers": KAFKA_BOOTSTRAP,
    "enable.idempotence": True,
})


def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")
    else:
        print(f"Delivered to {msg.topic()} [partition {msg.partition()}] offset {msg.offset()}")


def on_connect(client, userdata, flags, reason_code, properties):
    print(f"Connected to MQTT broker ({reason_code}). Subscribing to '{MQTT_TOPIC}'...")
    client.subscribe(MQTT_TOPIC, qos=1)


def on_message(client, userdata, msg):
    producer.produce(KAFKA_TOPIC, key=KAFKA_KEY, value=msg.payload, callback=delivery_report)
    producer.poll(0)


def main():
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="veloradar-mqtt-kafka-bridge")
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(MQTT_HOST, MQTT_PORT)

    try:
        client.loop_forever()
    except KeyboardInterrupt:
        print("Stopping bridge...")
    finally:
        producer.flush(10)
        client.disconnect()


if __name__ == "__main__":
    main()