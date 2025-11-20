"""
Dead Letter Queue (DLQ) Monitor.
Monitors and displays failed messages from the orders_dlq topic.
"""

import json
from confluent_kafka import DeserializingConsumer
from confluent_kafka.serialization import StringDeserializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer

import config


def load_avro_schema(schema_path):
    """Load Avro schema from file."""
    with open(schema_path, 'r') as f:
        return json.dumps(json.load(f))


def create_dlq_consumer():
    """
    Create and configure the DLQ consumer.
    
    Returns:
        DeserializingConsumer: Configured Kafka consumer for DLQ monitoring
    """
    schema_registry_client = SchemaRegistryClient({
        'url': config.SCHEMA_REGISTRY_URL
    })
    
    avro_schema_str = load_avro_schema('order.avsc')
    
    avro_deserializer = AvroDeserializer(
        schema_registry_client,
        avro_schema_str
    )
    
    consumer_config = {
        'bootstrap.servers': config.BOOTSTRAP_SERVERS,
        'key.deserializer': StringDeserializer('utf_8'),
        'value.deserializer': avro_deserializer,
        'group.id': 'dlq-monitor-group',
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': True
    }
    
    return DeserializingConsumer(consumer_config)


def main():
    """
    Main DLQ monitor loop.
    Displays all failed messages in the DLQ.
    """
    print("☠️ Starting Dead Letter Queue Monitor...")
    print(f"📡 Connected to: {config.BOOTSTRAP_SERVERS}")
    print(f"📋 Schema Registry: {config.SCHEMA_REGISTRY_URL}")
    print(f"📥 Monitoring DLQ topic: {config.ORDERS_DLQ_TOPIC}")
    print("-" * 60)
    
    consumer = create_dlq_consumer()
    consumer.subscribe([config.ORDERS_DLQ_TOPIC])
    
    message_count = 0
    
    try:
        print("\n⏳ Waiting for DLQ messages... (Press Ctrl+C to stop)\n")
        
        while True:
            msg = consumer.poll(timeout=1.0)
            
            if msg is None:
                continue
            
            if msg.error():
                print(f"❌ Consumer error: {msg.error()}")
                continue
            
            message_count += 1
            order = msg.value()
            
            print(f"☠️ [DLQ Message #{message_count}]")
            print(f"   OrderID: {order['orderId']}")
            print(f"   Product: {order['product']}")
            print(f"   Price: ${order['price']:.2f}")
            print(f"   📍 Partition: {msg.partition()} | Offset: {msg.offset()}")
            print(f"   ⏰ Timestamp: {msg.timestamp()[1]}")
            print()
            
    except KeyboardInterrupt:
        print("\n\n⏹️  DLQ Monitor stopped by user")
    except Exception as e:
        print(f"\n❌ DLQ Monitor error: {e}")
        raise
    finally:
        print("\n🔄 Closing DLQ monitor...")
        consumer.close()
        
        print("\n" + "=" * 60)
        print("📊 DLQ STATISTICS")
        print("=" * 60)
        print(f"Total Failed Messages in DLQ: {message_count}")
        print("=" * 60)
        print("\n✅ DLQ Monitor shut down gracefully")


if __name__ == '__main__':
    main()
