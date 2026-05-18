from confluent_kafka import Producer
import json
import os
import random
import time


config = {
    "bootstrap.servers": os.getenv("BOOTSTRAP_SERVERS", "46.225.20.89:9092"),
}

producer = Producer(config)
topic_name = "commerce-fhtw"

items = {
    "coffee-beans": (1.0, 8.0),
    "green-tea": (1.0, 5.0),
    "protein-bar": (5.0, 30.0),
    "notebook": (1.0, 12.0),
    "usb-cable": (1.0, 10.0),
    "water-bottle": (1.0, 6.0),
    "desk-lamp": (1.0, 3.0),
    "backpack": (1.0, 2.0),
}

cities = [
    {"city": "Vienna", "state": "Wien", "zipcode": 1010, "weight": 0.34},
    {"city": "Graz", "state": "Steiermark", "zipcode": 8010, "weight": 0.18},
    {"city": "Linz", "state": "Oberoesterreich", "zipcode": 4020, "weight": 0.14},
    {"city": "Salzburg", "state": "Salzburg", "zipcode": 5020, "weight": 0.10},
    {"city": "Innsbruck", "state": "Tirol", "zipcode": 6020, "weight": 0.09},
    {"city": "Klagenfurt", "state": "Kaernten", "zipcode": 9020, "weight": 0.08},
    {"city": "St. Poelten", "state": "Niederoesterreich", "zipcode": 3100, "weight": 0.07},
]


def generate_order(order_id):
    itemid = random.choice(list(items.keys()))
    min_units, max_units = items[itemid]
    city = random.choices(cities, weights=[entry["weight"] for entry in cities], k=1)[0]

    return {
        "ordertime": int(time.time() * 1000),
        "orderid": order_id,
        "itemid": itemid,
        "orderunits": round(random.uniform(min_units, max_units), 2),
        "address": {
            "city": city["city"],
            "state": city["state"],
            "zipcode": city["zipcode"],
        },
    }


def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")
    else:
        print(f"Order delivered to {msg.topic()} [{msg.partition()}] offset {msg.offset()}")


def produce_orders():
    order_id = random.randint(100000, 999999)

    while True:
        order = generate_order(order_id)
        producer.produce(
            topic_name,
            key=str(order_id),
            value=json.dumps(order),
            callback=delivery_report,
        )
        producer.poll(0)

        order_id += 1
        time.sleep(1)


if __name__ == "__main__":
    try:
        produce_orders()
    finally:
        producer.flush()
