# Kafka-Databricks Integration Guide

A practical guide for connecting Apache Kafka (Confluent Cloud) to Azure Databricks, including common pitfalls and solutions discovered during implementation.

---

## Table of Contents

- [Overview](#overview)
- [Prerequisites](#prerequisites)
- [Authentication Setup](#authentication-setup)
- [Connection Configuration](#connection-configuration)
- [Common Issues & Solutions](#common-issues--solutions)
- [Complete Working Example](#complete-working-example)
- [Best Practices](#best-practices)

---

## Overview

This guide covers connecting Databricks Structured Streaming to Confluent Cloud Kafka. The integration enables real-time data ingestion from Kafka topics into Delta Lake tables.

**Architecture:**
```
Confluent Cloud          Azure Databricks
┌─────────────────┐      ┌─────────────────────────────┐
│  Kafka Topic    │ ───▶ │  Structured Streaming Job   │
│  (SASL_SSL)     │      │           │                 │
└─────────────────┘      │           ▼                 │
                         │    Delta Lake Table         │
                         └─────────────────────────────┘
```

---

## Prerequisites

| Component | Requirement |
|-----------|-------------|
| Databricks Runtime | 11.3 LTS or higher (includes Kafka connector) |
| Confluent Cloud | Basic cluster or higher |
| Authentication | API Key + Secret from Confluent Cloud |
| Network | Databricks cluster must reach Confluent Cloud (public endpoint) |

---

## Authentication Setup

### Step 1: Create Confluent Cloud API Keys

1. Log into Confluent Cloud
2. Navigate to your cluster → **API Keys**
3. Click **Create Key** → **Global access**
4. Save both the **Key** and **Secret** immediately

### Step 2: Store Credentials in Databricks Secrets

Never hardcode credentials in notebooks. Use Databricks secret scopes.

**From terminal (Databricks CLI):**

```bash
# Create a secret scope
databricks secrets create-scope <scope-name> --profile <your-profile>

# Store each credential
databricks secrets put-secret <scope-name> CONFLUENT_BOOTSTRAP_SERVERS \
    --string-value "pkc-xxxxx.region.azure.confluent.cloud:9092" \
    --profile <your-profile>

databricks secrets put-secret <scope-name> CONFLUENT_API_KEY \
    --string-value "your-api-key" \
    --profile <your-profile>

databricks secrets put-secret <scope-name> CONFLUENT_API_SECRET \
    --string-value "your-api-secret" \
    --profile <your-profile>
```

**Verify secrets exist:**

```bash
databricks secrets list-secrets <scope-name> --profile <your-profile>
```

### Step 3: Retrieve Secrets in Notebooks

```python
kafka_bootstrap = dbutils.secrets.get(scope="<scope-name>", key="CONFLUENT_BOOTSTRAP_SERVERS")
kafka_api_key = dbutils.secrets.get(scope="<scope-name>", key="CONFLUENT_API_KEY")
kafka_api_secret = dbutils.secrets.get(scope="<scope-name>", key="CONFLUENT_API_SECRET")
```

> **Note:** Databricks automatically redacts secret values in notebook output. You cannot accidentally print them.

---

## Connection Configuration

### Critical: Shaded Dependencies

⚠️ **This is the most common integration failure.**

Databricks bundles a "shaded" version of the Kafka client, meaning package paths are renamed to avoid dependency conflicts.

**Standard Kafka (does NOT work in Databricks):**
```
org.apache.kafka.common.security.plain.PlainLoginModule
```

**Databricks shaded version (REQUIRED):**
```
kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule
```

### Working Configuration

```python
kafka_options = {
    # Connection
    "kafka.bootstrap.servers": kafka_bootstrap,
    
    # Security - SASL_SSL required for Confluent Cloud
    "kafka.security.protocol": "SASL_SSL",
    "kafka.sasl.mechanism": "PLAIN",
    
    # Authentication - USE SHADED CLASS PATH
    "kafka.sasl.jaas.config": f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="{kafka_api_key}" password="{kafka_api_secret}";',
    
    # Topic subscription
    "subscribe": "your-topic-name",
    
    # Where to start reading
    "startingOffsets": "earliest"  # or "latest"
}
```

### Configuration Options Explained

| Option | Description |
|--------|-------------|
| `kafka.bootstrap.servers` | Confluent Cloud broker address (ends in `:9092`) |
| `kafka.security.protocol` | `SASL_SSL` for encrypted + authenticated connection |
| `kafka.sasl.mechanism` | `PLAIN` for API key authentication |
| `kafka.sasl.jaas.config` | Java authentication config with credentials |
| `subscribe` | Topic name(s) to consume |
| `startingOffsets` | `earliest` = all messages; `latest` = only new |

---

## Common Issues & Solutions

### Issue 1: `No LoginModule found for org.apache.kafka.common.security.plain.PlainLoginModule`

**Cause:** Using standard Kafka class path instead of shaded version.

**Solution:** Change the JAAS config to use `kafkashaded.org.apache.kafka...`:

```python
# ❌ Wrong
"kafka.sasl.jaas.config": 'org.apache.kafka.common.security.plain.PlainLoginModule required ...'

# ✅ Correct
"kafka.sasl.jaas.config": 'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required ...'
```

---

### Issue 2: `Failed to create new KafkaAdminClient`

**Cause:** Usually authentication or network failure.

**Diagnosis:** Run a batch read test to get clearer errors:

```python
try:
    test_df = (
        spark.read
        .format("kafka")
        .options(**kafka_options)
        .option("endingOffsets", "latest")
        .load()
    )
    print(f"✅ Success! Found {test_df.count()} messages")
except Exception as e:
    print(f"❌ Failed:\n{e}")
```

**Common fixes:**
- Verify bootstrap server URL is correct
- Verify API key/secret are correct
- Check if Confluent Cloud cluster is running

---

### Issue 3: `NameError: name 'kafka_options' is not defined`

**Cause:** Cluster restarted or notebook session expired.

**Solution:** Re-run all cells from the top. Variables don't persist across cluster restarts.

**Tip:** Use **Run All Above** from a cell's menu to re-execute prerequisites.

---

### Issue 4: Stream Starts but No Data Arrives

**Possible causes:**

1. **Topic has no messages:** Check Confluent Cloud console → Topics → Messages
2. **Wrong `startingOffsets`:** Use `earliest` to read existing messages
3. **Consumer group already processed messages:** Delete checkpoint and restart

**Delete checkpoint (careful in production):**
```python
dbutils.fs.rm("/path/to/checkpoint", recurse=True)
```

---

## Complete Working Example

```python
# Cell 1: Configuration
KAFKA_TOPIC = "crypto-prices-raw"
BRONZE_TABLE = "crypto_analytics.bronze.raw_prices"
CHECKPOINT_PATH = "abfss://container@storage.dfs.core.windows.net/checkpoints/bronze"

# Cell 2: Get secrets
kafka_bootstrap = dbutils.secrets.get(scope="crypto-pipeline", key="CONFLUENT_BOOTSTRAP_SERVERS")
kafka_api_key = dbutils.secrets.get(scope="crypto-pipeline", key="CONFLUENT_API_KEY")
kafka_api_secret = dbutils.secrets.get(scope="crypto-pipeline", key="CONFLUENT_API_SECRET")

# Cell 3: Kafka options (with shaded class path!)
kafka_options = {
    "kafka.bootstrap.servers": kafka_bootstrap,
    "kafka.security.protocol": "SASL_SSL",
    "kafka.sasl.mechanism": "PLAIN",
    "kafka.sasl.jaas.config": f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="{kafka_api_key}" password="{kafka_api_secret}";',
    "subscribe": KAFKA_TOPIC,
    "startingOffsets": "earliest"
}

# Cell 4: Read stream
raw_stream = (
    spark.readStream
    .format("kafka")
    .options(**kafka_options)
    .load()
)

# Cell 5: Transform
from pyspark.sql.functions import col, current_timestamp

bronze_stream = (
    raw_stream
    .selectExpr(
        "CAST(key AS STRING) as message_key",
        "CAST(value AS STRING) as raw_json",
        "topic",
        "partition",
        "offset",
        "timestamp as kafka_timestamp"
    )
    .withColumn("bronze_ingested_at", current_timestamp())
)

# Cell 6: Write to Delta
bronze_query = (
    bronze_stream
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT_PATH)
    .toTable(BRONZE_TABLE)
)

# Cell 7: Monitor
spark.sql(f"SELECT COUNT(*) FROM {BRONZE_TABLE}").show()
```

---

## Best Practices

### Security

| Practice | Why |
|----------|-----|
| Use Databricks secret scopes | Credentials never appear in code or logs |
| Rotate API keys periodically | Limit blast radius of compromised keys |
| Use dedicated API keys per environment | Isolate dev/staging/prod |

### Reliability

| Practice | Why |
|----------|-----|
| Always use checkpoints | Enables exactly-once and restart recovery |
| Store checkpoints in cloud storage | Survives cluster termination |
| Use unique checkpoint per stream | Prevents conflicts between jobs |

### Performance

| Practice | Why |
|----------|-----|
| Match partitions to parallelism | 3 Kafka partitions = 3 Spark tasks reading |
| Use `earliest` only on first run | Avoids reprocessing on restart |
| Set reasonable `maxOffsetsPerTrigger` | Controls batch size |

### Monitoring

```python
# Check stream status
bronze_query.status

# Check recent progress
bronze_query.recentProgress

# Check for errors
bronze_query.exception()
```

---

## Additional Resources

- [Databricks Kafka Integration Docs](https://docs.databricks.com/structured-streaming/kafka.html)
- [Confluent Cloud Quick Start](https://docs.confluent.io/cloud/current/get-started/index.html)
- [Spark Structured Streaming Guide](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)

---

## Revision History

| Date | Change |
|------|--------|
| 2025-12-30 | Initial guide created during crypto pipeline project |
