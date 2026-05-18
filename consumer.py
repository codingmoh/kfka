from confluent_kafka import Consumer
import json
import os


config = {
    "bootstrap.servers": os.getenv("BOOTSTRAP_SERVERS", "46.225.20.89:9092"),
    "group.id": "commerce-fhtw-consumer",
    "auto.offset.reset": "earliest",
}

consumer = Consumer(config)
topic_name = "commerce-fhtw"


def main():
    consumer.subscribe([topic_name])
    print(f"Consuming from topic: {topic_name}")

    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                continue

            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue

            key = msg.key().decode("utf-8") if msg.key() else None
            value = msg.value().decode("utf-8")

            try:
                value = json.loads(value)
            except json.JSONDecodeError:
                pass

            print(
                {
                    "topic": msg.topic(),
                    "partition": msg.partition(),
                    "offset": msg.offset(),
                    "key": key,
                    "value": value,
                }
            )
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
