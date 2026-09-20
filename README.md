# Fintech Real-Time Fraud Detection Pipeline

To simulate enterprise-grade data engineering, this project builds a highly scalable real-time pipeline capable of handling thousands of financial transactions per second. The core challenge involved working with high-velocity data feeds, where network-induced transaction retries can cause duplicate submissions and poor cellular connectivity can introduce significant late-arriving event data.


## Objectives

1. Ingest thousands of real-time financial transaction records per second from a live public exchange feed (coinbase.com).
2. Enforce exactly-once processing concepts by identifying and dropping network duplicate records.
3. Account for network lag by cleanly integrating late-arriving data without causing unbounded state growth.
4. Execute velocity flags on high-risk transaction bursts (example: more than 3 distinct swipes per simulated card within a 1-minute window).
5. Persist data with fault-tolerant checkpointing and visualize the alerts dynamically.

## Architecture Overview

```
Coinbase WebSocket (live trades)
        │  producer.py — asyncio, reconnects with backoff
        ▼
Kafka topic "transactions"  (dual listener: localhost:9092 host / kafka:29092 in-cluster)
        │
        ▼
consumer_fraud_detection.py, submitted via spark-submit to
the spark-master / spark-worker cluster (Docker Compose)
   - 10-minute event-time watermark
   - dropDuplicates(transaction_id, timestamp)
   - 1-minute / 30s-slide windowed velocity aggregation
        │
        ▼
output/fraud_alerts (Parquet, bind-mounted so the host can read it)
+ output/checkpoints (fault-tolerant state recovery)
        │
        ▼
dashboard.py — Streamlit + Plotly live monitor
```

Kafka and the Spark cluster both run in Docker Compose; the consumer job is submitted to the `spark-master`/`spark-worker` containers with `spark-submit` rather than running as a local, unmanaged Spark session. The producer runs on the host as an asyncio WebSocket client, and the dashboard runs on the host reading the same Parquet output directory the cluster writes to via a shared bind mount.

## Simulated Fraud-Detection Methodology

Coinbase's public feed has no concept of a "card" or a cardholder — it only reports asset trades. To make the velocity-detection logic mechanically meaningful (rather than just a proxy for trading volume on popular pairs), each incoming trade is assigned a synthetic per-transaction card/account id: a stable hash of the transaction id, bucketed into a fixed pool (`CARD_0001`..`CARD_NNNN`, pool size configurable), kept **independent of which asset was traded** (tracked separately as `asset`). Velocity is then computed per synthetic card, not per asset.

This means the pipeline demonstrates the *mechanics* of a real velocity-based fraud rule — many events against the same entity in a short window — running on real market-timing data with synthetic entity identities layered on top. **It is not real fraud detection on real cardholders or real payment-network data**, and the README does not claim otherwise.

## Data Schema

**Transaction (producer → Kafka → consumer):**

| Field | Type | Notes |
|---|---|---|
| `transaction_id` | string | `TXN_<coinbase sequence number>` |
| `card_number` | string | synthetic id, `CARD_0001`..`CARD_NNNN`, independent of `asset` |
| `asset` | string | traded pair, e.g. `BTC-USD` |
| `timestamp` | string / TimestampType | trade time |
| `amount` | double | price × size |
| `merchant_id` | string | `EXCHANGE_BUY` / `EXCHANGE_SELL` |
| `location` | string | `GLOBAL_NET` |

**Alert (consumer output):**

| Field | Notes |
|---|---|
| `alert_window_start` / `alert_window_end` | 1-minute window, 30s slide |
| `card_number` | the flagged synthetic card |
| `transaction_count` | count within the window (threshold: > 3) |
| `assets_involved` | distinct assets that card traded within the window |
| `alert_triggered_at` | when the alert was produced |

## Resilience & Fault-Tolerance

- **Producer reconnection**: the WebSocket client reconnects automatically with backoff on connection drops, and re-subscribes on each new connection.
- **No silent message loss on shutdown**: the Kafka producer is flushed on shutdown and on reconnect, so buffered-but-unsent messages aren't dropped.
- **Late data**: a 10-minute event-time watermark bounds how long state is kept, so late-but-within-bound events still land correctly while excessively late events are dropped instead of growing state unboundedly.
- **Duplicates**: `dropDuplicates(["transaction_id", "timestamp"])` within the watermarked window removes network-retry duplicates.
- **Checkpointing**: Spark Structured Streaming checkpoints to `output/checkpoints`, allowing the job to resume state after a restart.
- **Dashboard read robustness**: corrupt, empty, or partially-written Parquet files are skipped and retried on the next poll rather than crashing the dashboard.

Known limitation: because the alert sink is local-filesystem Parquet (not a distributed filesystem), correct checkpointing depends on exactly one Spark worker sharing the bind-mounted output path — see **Known Limitations** below.

## Prerequisites

- Docker Desktop with Compose v2
- Python **3.10–3.12** (this repo's own `venv/` currently defaults to a newer interpreter than that — recreate it on 3.10–3.12 before installing dependencies)
- Java **17** (Temurin recommended) for running PySpark locally, e.g. for `pytest` or `read_results.py` outside the cluster (the host's system-default JVM may be a different version — point `JAVA_HOME` at a 17 install for this project)

## Setup & Run

1. Start the infrastructure:
   ```
   docker compose up -d
   ```
2. Create and activate a Python 3.10–3.12 virtual environment, then install dependencies:
   ```
   pip install -r requirements-dev.txt
   ```
3. Submit the consumer job to the Spark cluster (run in its own terminal — it's a long-lived streaming query):
   ```
   docker compose exec -w /opt/spark-apps spark-master spark-submit \
     --master spark://spark-master:7077 --deploy-mode client \
     --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.2 \
     consumer_fraud_detection.py --bootstrap-servers kafka:29092
   ```
4. Start the producer (host machine):
   ```
   python producer.py
   ```
5. Start the dashboard:
   ```
   streamlit run dashboard.py
   ```
6. (Optional) Run the demonstration scripts to see deduplication and late-data handling in action:
   ```
   python demo/inject_duplicates.py
   python demo/inject_late_data.py
   ```
7. Run the test suite (no Docker required — pure batch/unit tests):
   ```
   pytest -v
   ```

## Verification

- **Deduplication**: `demo/inject_duplicates.py` prints the expected post-dedup transaction count for a target card/window; compare it against the actual count in `output/fraud_alerts` (or via `read_results.py`) once the window closes.
- **Late data**: `demo/inject_late_data.py` sends on-time, late-but-within-watermark, and beyond-watermark events; the within-watermark window's alert should appear, the beyond-watermark window's alert should be absent even though enough raw events were sent to cross the threshold.
- **Cluster wiring**: `docker compose ps` shows `spark-master`/`spark-worker` running; `http://localhost:8080` shows one live worker and the submitted streaming application.
- **Dashboard**: shows an explicit "waiting for data" state before any alerts exist (never fabricated numbers), and populates with real alerts once the job writes output.
- **Tests/CI**: `pytest -v` passes locally with no Docker running; the GitHub Actions workflow runs the same suite on every push/PR.

## Known Limitations

- Output is plain Apache Parquet, not Delta Lake — no ACID transaction log, schema enforcement, or time travel; the output directory is named accordingly (`output/fraud_alerts`, not "delta").
- Local-filesystem checkpointing/output is correct only with a single Spark worker sharing the bind-mounted path; scaling worker replicas would require a distributed filesystem (e.g. HDFS/S3), which is out of scope for this local demo.
- `card_number` values are simulated per-transaction identities, not real cardholder data.
- Continuous integration runs the unit/batch test suite only; it does not spin up Docker Compose or exercise the live Kafka/Spark cluster.

## Result

The pipeline absorbs and processes rapid micro-batches under high-volume load conditions, capturing extreme transaction-velocity events for high-activity assets like BTC-USD, with state-store bounds and fault tolerance provided by watermark-based state purges and checkpointing.

