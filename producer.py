import json
import time
import random
import uuid
from confluent_kafka import SerializingProducer
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

import config


PRODUCTS = [
    "Laptop", "Mouse", "Keyboard", "Monitor", "Headphones",
    "Webcam", "USB Cable", "External SSD", "Docking Station", "Chair"
]


def load_avro_schema(schema_path):
    """Load Avro schema from file."""
    with open(schema_path, 'r') as f:
        return json.load(f)


def generate_order():
    """
    Generate a synthetic order with random data.
    
    Returns:
        dict: Order data matching the Avro schema
    """
    return {
        'orderId': str(uuid.uuid4()),
        'product': random.choice(PRODUCTS),
        'price': round(random.uniform(9.99, 999.99), 2)
    }


def delivery_report(err, msg):
    """
    Callback for message delivery reports.
    Called once for each message produced to indicate delivery result.
    """
    if err is not None:
        print(f'❌ Message delivery failed: {err}')
    else:
        print(f'✅ Message delivered to {msg.topic()} [partition {msg.partition()}] at offset {msg.offset()}')


def create_producer():
    """
    Create and configure the Avro Producer.
    
    Returns:
        SerializingProducer: Configured Kafka producer with Avro serialization
    """
    schema_registry_client = SchemaRegistryClient({
        'url': config.SCHEMA_REGISTRY_URL
    })
    
    avro_schema = load_avro_schema('order.avsc')
    avro_schema_str = json.dumps(avro_schema)
    
    avro_serializer = AvroSerializer(
        schema_registry_client,
        avro_schema_str
    )
    
    producer_config = {
        'bootstrap.servers': config.BOOTSTRAP_SERVERS,
        'key.serializer': StringSerializer('utf_8'),
        'value.serializer': avro_serializer,
        'acks': config.PRODUCER_ACKS
    }
    
    return SerializingProducer(producer_config)


def main():
    """
    Main producer loop.
    Generates and sends one order per second indefinitely.
    """
    print("🚀 Starting Kafka Avro Producer...")
    print(f"📡 Connected to: {config.BOOTSTRAP_SERVERS}")
    print(f"📋 Schema Registry: {config.SCHEMA_REGISTRY_URL}")
    print(f"📨 Publishing to topic: {config.ORDERS_TOPIC}")
    print("-" * 60)
    
    producer = create_producer()
    message_count = 0
    
    try:
        while True:
            order = generate_order()
            message_count += 1
            
            print(f"\n📦 [Message #{message_count}] Producing order:")
            print(f"   OrderID: {order['orderId']}")
            print(f"   Product: {order['product']}")
            print(f"   Price: ${order['price']:.2f}")
            
            producer.produce(
                topic=config.ORDERS_TOPIC,
                key=order['orderId'],
                value=order,
                on_delivery=delivery_report
            )
            
            producer.poll(0)
            
            time.sleep(1)
            
    except KeyboardInterrupt:
        print("\n\n⏹️  Producer stopped by user")
    except Exception as e:
        print(f"\n❌ Producer error: {e}")
        raise
    finally:
        print("\n🔄 Flushing remaining messages...")
        producer.flush()
        print(f"✅ Producer shut down gracefully. Total messages sent: {message_count}")


if __name__ == '__main__':
    main()
