from confluent_kafka import Producer
import json
import os
import random
import time


config = {
    "bootstrap.servers": os.getenv("BOOTSTRAP_SERVERS", "46.225.20.89:9092"),
}

producer = Producer(config)
topic_name = os.getenv("MUSIC_TOPIC", "music-fhtw")
input_path = os.getenv("MUSIC_INPUT_FILE", "output.json")
output_path = os.getenv("MUSIC_OUTPUT_FILE", "newoutput.json")


def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")
    else:
        print(f"Event delivered to {msg.topic()} [{msg.partition()}] offset {msg.offset()}")


def load_events():
    with open(input_path, "r") as file:
        return json.load(file)


def prepare_events(events):
    split_index = len(events) // 3

    first_third = [
        {
            **entry,
            "userId": str(random.randint(1, 10)),
            "page": "NextSong",
        }
        for entry in events[:split_index]
    ]

    remaining_events = [
        {
            **entry,
            "userId": str(random.randint(1050, 1100)),
            "page": random.choice(["Skip", "NextSong"]),
        }
        for entry in events[split_index:]
    ]

    return first_third + remaining_events


def write_json_lines(events):
    with open(output_path, "w") as file:
        for entry in events:
            json.dump(entry, file)
            file.write("\n")


def produce_events(events):
    while True:
        for entry in events:
            record_value = json.dumps(entry)
            record_key = entry["userId"]
            producer.produce(
                topic_name,
                key=record_key,
                value=record_value,
                callback=delivery_report,
            )
            producer.poll(0)

        producer.flush()
        time.sleep(5)


if __name__ == "__main__":
    try:
        modified_events = prepare_events(load_events())
        write_json_lines(modified_events)
        produce_events(modified_events)
    finally:
        producer.flush()
