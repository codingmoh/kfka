# Redpanda Kafka Producers

This project runs a local Redpanda Kafka-compatible broker and three Python producers.

## Requirements

- Docker and Docker Compose
- Python 3.12+
- `uv` or `pip`

## Start Kafka

Start the Redpanda broker:

```bash
docker compose up -d
```

The broker is exposed on:

```text
46.225.20.89:9092
```

The producer scripts use this address by default. To point them somewhere else, set `BOOTSTRAP_SERVERS`:

```bash
export BOOTSTRAP_SERVERS=localhost:9092
```

Stop the broker:

```bash
docker compose down
```

## Install Dependencies

Using `uv`:

```bash
uv sync
```

Or using `pip`:

```bash
pip install -r requirements.txt
```

## Run Producers

Run each producer in its own terminal.

### Stock Trades

Publishes generated stock trade events to `fhtw-stocks`.

```bash
uv run python producer.py
```

Or:

```bash
python producer.py
```

### Music Events

Reads events from `output.json`, writes modified JSON lines to `newoutput.json`, and publishes events to `music-fhtw`.

```bash
uv run python music_producer.py
```

Or:

```bash
python music_producer.py
```

Optional environment variables:

```bash
export MUSIC_TOPIC=music-fhtw
export MUSIC_INPUT_FILE=output.json
export MUSIC_OUTPUT_FILE=newoutput.json
```

### Commerce Orders

Publishes generated commerce order events to `commerce-fhtw`.

```bash
uv run python commerce_producer.py
```

Or:

```bash
python commerce_producer.py
```

## Run All Producers

Open three terminals and run:

```bash
uv run python producer.py
uv run python music_producer.py
uv run python commerce_producer.py
```

Each producer runs continuously until stopped with `Ctrl+C`.

## Optional Consumer

The included consumer reads from `commerce-fhtw`:

```bash
uv run python consumer.py
```
