from confluent_kafka import Producer
import json
import os
import random
import time


config = {
    "bootstrap.servers": os.getenv("BOOTSTRAP_SERVERS", "46.225.20.89:9092"),
}

producer = Producer(config)
topic_name = "fhtw-stocks"

stocks = {
    "AAPL": 0.5,
    "MSFT": 0.3,
    "GOOGL": 0.1,
    "AMZN": 0.03,
    "TSLA": 0.02,
    "META": 0.01,
    "NFLX": 0.01,
    "NVDA": 0.01,
    "AMD": 0.01,
    "INTC": 0.01,
}

accounts = ["ACC123", "ACC456", "ACC789", "ACC012", "ACC345"]
users = ["User_1", "User_2", "User_3", "User_4", "User_5"]


def generate_trade():
    symbol = random.choices(list(stocks.keys()), weights=stocks.values(), k=1)[0]

    if symbol == "AAPL":
        side = "SELL"
        quantity = random.randint(500, 1000)
    elif symbol == "MSFT":
        side = "BUY"
        quantity = random.randint(300, 700)
    else:
        side = random.choice(["BUY", "SELL"])
        quantity = random.randint(1, 500)

    price = random.uniform(50, 2000) if symbol in ["AAPL", "MSFT"] else random.uniform(100, 1500)

    return {
        "side": side,
        "quantity": quantity,
        "symbol": symbol,
        "price": round(price, 2),
        "account": random.choice(accounts),
        "userid": random.choice(users),
    }


def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}] offset {msg.offset()}")


def produce_trades(num_messages=100):
    for _ in range(num_messages):
        trade = generate_trade()
        key = str(random.randint(1050, 1100))
        producer.produce(
            topic_name,
            key=key,
            value=json.dumps(trade),
            callback=delivery_report,
        )
        producer.poll(0)
        time.sleep(1)

    producer.flush()


if __name__ == "__main__":
    while True:
        produce_trades(100)
        time.sleep(5)
