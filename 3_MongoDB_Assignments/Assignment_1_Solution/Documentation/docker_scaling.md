# Task 5 — Scaling Kafka Consumers with Docker

## 1. Objective

The objective of this task is to demonstrate how multiple Kafka consumer instances can be run using Docker and how Kafka distributes topic partitions among consumers belonging to the same consumer group.

The `logistics_data` Kafka topic has **4 partitions**.

The consumer application:

- Reads messages from the `logistics_data` Kafka topic.
- Deserializes Avro messages using Confluent Schema Registry.
- Validates and cleans the records.
- Inserts valid records into MongoDB.
- Uses manual Kafka offset commits after successful processing.

---

## 2. Docker Setup

The consumer application is packaged into a Docker image.

### Project files

```text
Assignment_1_Solution/
├── logistics_data_consumer.py
├── requirements.txt
└── Dockerfile
```

### `requirements.txt`

```text
confluent-kafka
pymongo
certifi
httpx
authlib
cachetools
attrs
fastavro
```

### Dockerfile

```dockerfile
FROM python:3.14-slim

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY logistics_data_consumer.py .

CMD ["python", "-u", "logistics_data_consumer.py"]
```

The `-u` option runs Python in unbuffered mode so that consumer output is displayed in real time when running inside Docker.

---

## 3. Building the Docker Image

The Docker image is built using:

```bash
docker build -t logistics-consumer .
```

The image contains:

- Python 3.14
- Required Python dependencies
- `logistics_data_consumer.py`

The image can then be used to create multiple independent consumer containers.

---

## 4. Running a Consumer Container

The Kafka, Schema Registry, and MongoDB credentials are provided to the container through environment variables.

Example:

```bash
docker run --name logistics-consumer-1 \
  -e KAFKA_URL="$KAFKA_URL" \
  -e KAFKA_USERNAME="$KAFKA_USERNAME" \
  -e KAFKA_PASSWORD="$KAFKA_PASSWORD" \
  -e SCHEMA_REGISTRY_URL="$SCHEMA_REGISTRY_URL" \
  -e SCHEMA_REGISTRY_API_KEY="$SCHEMA_REGISTRY_API_KEY" \
  -e SCHEMA_REGISTRY_API_SECRET="$SCHEMA_REGISTRY_API_SECRET" \
  -e MONGODB_URI="$MONGODB_URI" \
  logistics-consumer
```

The credentials are not hard-coded into the Docker image.

Additional consumer containers can be created from the same image by changing only the container name:

```text
logistics-consumer-1
logistics-consumer-2
logistics-consumer-3
logistics-consumer-4
```

---

## 5. Kafka Consumer Group

All consumer containers use the same Kafka consumer group:

```python
CONSUMER_GROUP = "logistics-consumer"
```

This is important because Kafka distributes partitions among consumers **within the same consumer group**.

The Docker containers themselves do not decide which Kafka messages each consumer receives. Kafka manages the partition assignment.

---

## 6. Topic and Partition Configuration

The Kafka topic is:

```text
logistics_data
```

The topic has:

```text
4 partitions
```

Therefore, the maximum number of consumers that can actively consume partitions simultaneously within the same consumer group is **4**.

Conceptually:

```text
Kafka Topic: logistics_data

Partition 0 ──→ Consumer
Partition 1 ──→ Consumer
Partition 2 ──→ Consumer
Partition 3 ──→ Consumer
```

---

## 7. Scaling Experiments

### 7.1 One Consumer

With one consumer:

```text
4 partitions
      ↓
Consumer 1
```

A single consumer can consume from all four partitions.

---

### 7.2 Two Consumers

Two Docker containers were started using the same consumer group:

```text
Consumer 1
Consumer 2
```

Both consumers successfully consumed messages from Kafka and inserted records into MongoDB.

Kafka distributed the four partitions between the two consumers.

Conceptually:

```text
Partition 0 ──→ Consumer 1
Partition 1 ──→ Consumer 2
Partition 2 ──→ Consumer 1
Partition 3 ──→ Consumer 2
```

The exact partition assignment is controlled by Kafka and may differ.

During the test, the MongoDB collection increased from:

```text
38 documents
```

to:

```text
71 documents
```

This confirmed that both consumer instances were actively processing records.

---

### 7.3 Four Consumers

Four Docker containers were then started:

```text
Consumer 1
Consumer 2
Consumer 3
Consumer 4
```

All four consumers successfully consumed messages and inserted records into MongoDB.

With four partitions and four consumers, Kafka can assign one partition to each consumer:

```text
Partition 0 ──→ Consumer 1
Partition 1 ──→ Consumer 2
Partition 2 ──→ Consumer 3
Partition 3 ──→ Consumer 4
```

The exact assignment is determined by Kafka.

---

### 7.4 Five Consumers

A fifth consumer was started:

```text
Consumer 1
Consumer 2
Consumer 3
Consumer 4
Consumer 5
```

Since the topic has only four partitions, all five consumers cannot actively consume a partition simultaneously.

Initially, one consumer remained idle.

```text
4 partitions
     ↓
4 active consumers
     +
1 idle consumer
```

However, starting the fifth consumer caused a consumer-group rebalance. Kafka recalculated the partition assignment, and in one observed assignment, Consumer 5 received a partition while another consumer became idle.

This demonstrated that the assignment is dynamically managed by Kafka rather than permanently fixed.

---

## 8. Consumer Rebalancing

Consumer rebalancing was also tested manually.

A consumer was stopped while multiple consumers were running.

The following sequence was observed:

```text
Consumer leaves
      ↓
Kafka detects the membership change
      ↓
Consumer group rebalance
      ↓
Partitions reassigned
      ↓
Another consumer receives the partition
```

For example, when Consumer 5 was stopped and its Docker container was subsequently removed, Kafka rebalanced the consumer group and Consumer 2 became active again and started consuming messages.

This demonstrated that an idle consumer can become active when a partition becomes available.

---

## 9. Important Observation About Stopping Containers

Pressing:

```text
Ctrl + C
```

stops the Python consumer process.

However, the Docker container and the application process should be considered separately when troubleshooting consumer-group behavior.

During testing, stopping the Python process alone did not immediately result in the expected reassignment. Once the stopped Consumer 5 container was removed, Kafka detected the consumer-group membership change and rebalancing occurred.

Therefore, the observed sequence was:

```text
Ctrl + C
    ↓
Python consumer stops

Container removed
    ↓
Kafka detects consumer departure

Rebalance
    ↓
Partition reassigned
```

---

## 10. Scaling Rule

For a Kafka topic with `N` partitions:

```text
Maximum active consumers in one consumer group = N
```

For this assignment:

```text
Partitions = 4
```

Therefore:

```text
Consumers    Result
---------    --------------------------------
1            One consumer can handle all 4 partitions
2            Partitions distributed across 2 consumers
3            Partitions distributed across 3 consumers
4            Up to 4 active consumers
5+           Some consumers remain idle
```

Adding consumers beyond the number of partitions does not increase parallelism for that topic within the same consumer group.

---

## 11. Docker and Kafka Responsibilities

Docker and Kafka have different responsibilities.

### Docker

Docker provides isolated instances of the consumer application:

```text
Docker Image
     ↓
Container 1
Container 2
Container 3
Container 4
```

### Kafka

Kafka manages:

- Consumer group membership
- Partition assignment
- Consumer rebalancing
- Distribution of partitions among consumers

Therefore:

```text
Docker
  ↓
Creates multiple consumer instances
  ↓
Kafka Consumer Group
  ↓
Assigns partitions to consumers
```

Docker itself does not distribute Kafka messages between containers.

---

## 12. Viewing Container Output

A running container's output can be viewed directly when the container is attached to the terminal.

For a container running in the background, Docker logs can be followed using:

```bash
docker logs -f logistics-consumer-1
```

The `-f` option continuously follows new log output.

---

## 13. Task 5 Conclusion

The Docker-based Kafka consumer scaling was successfully tested.

The experiment demonstrated:

- Creating a Docker image for the Kafka consumer.
- Running multiple containers from the same image.
- Passing Kafka, Schema Registry, and MongoDB configuration through environment variables.
- Running multiple consumers in the same Kafka consumer group.
- Distributing Kafka partitions among consumers.
- The relationship between Kafka partitions and consumer parallelism.
- Consumers becoming idle when there are more consumers than partitions.
- Consumer-group rebalancing when consumers join or leave.
- A previously idle consumer becoming active after a partition becomes available.

The practical experiment confirmed that **Docker provides the consumer instances while Kafka manages partition assignment and rebalancing within the consumer group**.
