
# lê o tópico do Kafka, junta os eventos em lotes e envia cada lote como um arquivo JSONL para o Volume.
#
# Como ele funciona:
#
#  Lote: envia um arquivo quando junta 50 eventos ou quando o lote mais antigo completa 30 segundos, o que vier primeiro. 
#        Assim não cria um arquivo por passagem
#
#  Consumer group (veloradar-databricks-uploader): o Kafka lembra até onde esse programa já leu. 
#                                                  Se você parar e rodar de novo, ele continua de onde parou, sem reenviar tudo
#
#  auto.offset.reset: earliest: na primeira vez, lê o tópico desde o começo. Então os 14 eventos de ontem vão no primeiro arquivo
#
#  Commit só depois do upload: o programa só avisa o Kafka “já processei” depois que o arquivo chegou no Databricks. 
#                              Se o upload falhar, os eventos são lidos de novo. 
#                              Pode gerar duplicata, mas nunca perde dado, e a silver remove as duplicatas
#
#  kafka_key, kafka_partition, kafka_offset: acrescenta a origem de cada evento no Kafka. 
#                                            O offset é único por evento, então vai ser a chave perfeita para deduplicar na silver
#
#  Nome do arquivo: inclui data/hora e a faixa de offsets, por exemplo events_20261121T105500Z_offsets_0-13.jsonl. Fácil de rastrear
#
#  Ctrl + C: antes de sair, envia o que estiver no lote, para não ficar nada para trás

import io
import json
import os
import time
from datetime import datetime, timezone

from confluent_kafka import Consumer
from databricks.sdk import WorkspaceClient

KAFKA_BOOTSTRAP = "localhost:9092"
KAFKA_TOPIC = "veloradar-events"
KAFKA_GROUP_ID = "veloradar-databricks-uploader"

VOLUME_PATH = os.environ.get("VELORADAR_VOLUME_PATH", "/Volumes/veloradar_dev/landing/events")

BATCH_MAX_EVENTS = 50
BATCH_MAX_SECONDS = 30


def upload_batch(client, batch):
    first_offset = batch[0]["kafka_offset"]
    last_offset = batch[-1]["kafka_offset"]
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    filename = f"events_{timestamp}_offsets_{first_offset}-{last_offset}.jsonl"

    content = "".join(json.dumps(event) + "\n" for event in batch)
    client.files.upload(f"{VOLUME_PATH}/{filename}", io.BytesIO(content.encode("utf-8")), overwrite=True)
    print(f"Uploaded {len(batch)} events to {filename}")


def main():
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": KAFKA_GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe([KAFKA_TOPIC])

    client = WorkspaceClient()
    print(f"Connected to Databricks as {client.current_user.me().user_name}")
    print(f"Reading '{KAFKA_TOPIC}' and uploading to {VOLUME_PATH}...")

    batch = []
    batch_started_at = time.time()

    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is not None:
                if msg.error():
                    print(f"Kafka error: {msg.error()}")
                    continue

                try:
                    event = json.loads(msg.value())
                except json.JSONDecodeError:
                    print(f"Skipped invalid message at offset {msg.offset()}")
                    continue

                event["kafka_key"] = msg.key().decode("utf-8") if msg.key() else None
                event["kafka_partition"] = msg.partition()
                event["kafka_offset"] = msg.offset()

                if not batch:
                    batch_started_at = time.time()
                batch.append(event)

            batch_is_full = len(batch) >= BATCH_MAX_EVENTS
            batch_is_old = batch and time.time() - batch_started_at >= BATCH_MAX_SECONDS

            if batch_is_full or batch_is_old:
                upload_batch(client, batch)
                consumer.commit(asynchronous=False)
                batch = []

    except KeyboardInterrupt:
        print("Stopping uploader...")
    finally:
        if batch:
            upload_batch(client, batch)
            consumer.commit(asynchronous=False)
        consumer.close()


if __name__ == "__main__":
    main()