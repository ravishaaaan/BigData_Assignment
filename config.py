# Configuration settings for the Kafka-based order processing application
BOOTSTRAP_SERVERS = 'localhost:9092'

SCHEMA_REGISTRY_URL = 'http://localhost:8081'

ORDERS_TOPIC = 'orders'
ORDERS_DLQ_TOPIC = 'orders_dlq'

CONSUMER_GROUP_ID = 'order-processor-group'
AUTO_OFFSET_RESET = 'earliest'

PRODUCER_ACKS = 'all'

MAX_RETRIES = 3
RETRY_BACKOFF_SECONDS = 1

ERROR_SIMULATION_RATE = 0.1

