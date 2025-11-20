"""
Kafka Avro Consumer for Order Messages.
Consumes orders from the 'orders' topic and calculates running average of prices.
Includes error simulation, retry logic with exponential backoff, and Dead Letter Queue.
"""

import json
import random
import time
from confluent_kafka import DeserializingConsumer, SerializingProducer
from confluent_kafka.serialization import StringDeserializer, StringSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer

import config


class RunningAverageCalculator:
    """
    Maintains running average of order prices.
    Uses incremental calculation to avoid storing all values.
    """
    
    def __init__(self):
        self.count = 0
        self.sum = 0.0
        self.average = 0.0
    
    def add_price(self, price):
        """
        Add a new price and update the running average.
        
        Args:
            price (float): The price to add
            
        Returns:
            float: The updated running average
        """
        self.count += 1
        self.sum += price
        self.average = self.sum / self.count
        return self.average
    
    def get_stats(self):
        """
        Get current statistics.
        
        Returns:
            dict: Statistics including count, sum, and average
        """
        return {
            'count': self.count,
            'sum': round(self.sum, 2),
            'average': round(self.average, 2)
        }


def load_avro_schema(schema_path):
    """Load Avro schema from file."""
    with open(schema_path, 'r') as f:
        return json.dumps(json.load(f))


def create_dlq_producer():
    """
    Create a producer for sending failed messages to the Dead Letter Queue.
    
    Returns:
        SerializingProducer: Configured Kafka producer for DLQ
    """
    schema_registry_client = SchemaRegistryClient({
        'url': config.SCHEMA_REGISTRY_URL
    })
    
    avro_schema_str = load_avro_schema('order.avsc')
    
    avro_serializer = AvroSerializer(
        schema_registry_client,
        avro_schema_str
    )
    
    producer_config = {
        'bootstrap.servers': config.BOOTSTRAP_SERVERS,
        'key.serializer': StringSerializer('utf_8'),
        'value.serializer': avro_serializer,
        'acks': 'all'
    }
    
    return SerializingProducer(producer_config)


def send_to_dlq(dlq_producer, order, error_message, error_type):
    """
    Send a failed message to the Dead Letter Queue.
    
    Args:
        dlq_producer: The DLQ producer instance
        order (dict): The failed order message
        error_message (str): The error message/reason
        error_type (str): The type of error (ValueError, ConnectionError, etc.)
    """
    try:
        dlq_producer.produce(
            topic=config.ORDERS_DLQ_TOPIC,
            key=order['orderId'],
            value=order
        )
        
        dlq_producer.flush()
        
        print(f"   ☠️ Sent to DLQ: {config.ORDERS_DLQ_TOPIC}")
        print(f"   Reason: {error_type} - {error_message}")
        
    except Exception as e:
        print(f"   ❌ CRITICAL: Failed to send to DLQ: {e}")


def create_consumer():
    """
    Create and configure the Avro Consumer.
    
    Returns:
        DeserializingConsumer: Configured Kafka consumer with Avro deserialization
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
        'group.id': config.CONSUMER_GROUP_ID,
        'auto.offset.reset': config.AUTO_OFFSET_RESET,
        'enable.auto.commit': True,
        'auto.commit.interval.ms': 5000
    }
    
    return DeserializingConsumer(consumer_config)


def process_message(order, calculator):
    """
    Process an order message and update the running average.
    
    This function includes error simulation (chaos monkey) to test
    fault tolerance mechanisms in Steps 6 & 7.
    
    Args:
        order (dict): The deserialized order message
        calculator (RunningAverageCalculator): The calculator instance
        
    Returns:
        float: The updated running average
        
    Raises:
        ValueError: Simulated data validation error (non-retryable)
        ConnectionError: Simulated temporary service error (retryable)
    """
    if random.random() < config.ERROR_SIMULATION_RATE:
        error_type = random.choice(['validation', 'connection'])
        
        if error_type == 'validation':
            raise ValueError(
                f"❌ SIMULATED ERROR: Invalid data for order {order['orderId']} - "
                f"Price validation failed: {order['price']}"
            )
        else:
            raise ConnectionError(
                f"⚠️ SIMULATED ERROR: Temporary service unavailable while processing "
                f"order {order['orderId']}"
            )
    
    price = order['price']
    new_average = calculator.add_price(price)
    
    return new_average


def process_message_with_retry(order, calculator):
    """
    Process a message with retry logic for transient errors.
    
    Implements exponential backoff retry pattern:
    - Retryable errors (ConnectionError): Retry up to MAX_RETRIES times
    - Non-retryable errors (ValueError): Raise immediately without retry
    
    Args:
        order (dict): The deserialized order message
        calculator (RunningAverageCalculator): The calculator instance
        
    Returns:
        float: The updated running average
        
    Raises:
        ValueError: Non-retryable error (poison pill)
        ConnectionError: After MAX_RETRIES exhausted
    """
    attempt = 0
    last_error = None
    
    while attempt <= config.MAX_RETRIES:
        try:
            return process_message(order, calculator)
            
        except ValueError as e:
            print(f"   ❌ Non-retryable error (ValueError) - will not retry")
            raise
            
        except ConnectionError as e:
            last_error = e
            attempt += 1
            
            if attempt <= config.MAX_RETRIES:
                backoff = config.RETRY_BACKOFF_SECONDS * (2 ** (attempt - 1))
                print(f"   ⚠️ Retryable error (ConnectionError) - Attempt {attempt}/{config.MAX_RETRIES}")
                print(f"   ⏳ Retrying in {backoff} seconds...")
                time.sleep(backoff)
            else:
                print(f"   ❌ Max retries ({config.MAX_RETRIES}) exhausted")
                raise
    
    if last_error:
        raise last_error


def main():
    """
    Main consumer loop.
    Reads orders, calculates running average, handles errors with retry and DLQ.
    """
    print("🚀 Starting Kafka Avro Consumer...")
    print(f"📡 Connected to: {config.BOOTSTRAP_SERVERS}")
    print(f"📋 Schema Registry: {config.SCHEMA_REGISTRY_URL}")
    print(f"📥 Consuming from topic: {config.ORDERS_TOPIC}")
    print(f"👥 Consumer Group: {config.CONSUMER_GROUP_ID}")
    print(f"☠️ DLQ Topic: {config.ORDERS_DLQ_TOPIC}")
    print("-" * 60)
    
    consumer = create_consumer()
    consumer.subscribe([config.ORDERS_TOPIC])
    
    dlq_producer = create_dlq_producer()
    
    calculator = RunningAverageCalculator()
    message_count = 0
    dlq_count = 0
    
    try:
        print("\n⏳ Waiting for messages... (Press Ctrl+C to stop)\n")
        
        while True:
            msg = consumer.poll(timeout=1.0)
            
            if msg is None:
                continue
            
            if msg.error():
                print(f"❌ Consumer error: {msg.error()}")
                continue
            
            message_count += 1
            order = msg.value()
            
            try:
                new_average = process_message_with_retry(order, calculator)
                
                print(f"📦 [Message #{message_count}] Consumed order:")
                print(f"   OrderID: {order['orderId']}")
                print(f"   Product: {order['product']}")
                print(f"   Price: ${order['price']:.2f}")
                print(f"   🔢 Running Average: ${new_average:.2f} (from {calculator.count} orders)")
                print(f"   📍 Partition: {msg.partition()} | Offset: {msg.offset()}")
                print()
                
            except ConnectionError as e:
                dlq_count += 1
                print(f"🔥 RETRY EXHAUSTED - ConnectionError:")
                print(f"   {str(e)}")
                print(f"   OrderID: {order['orderId']}")
                
                send_to_dlq(dlq_producer, order, str(e), 'ConnectionError')
                print(f"   ✅ Offset will be committed - moving to next message")
                print()
                
            except ValueError as e:
                dlq_count += 1
                print(f"🔥 POISON PILL - ValueError:")
                print(f"   {str(e)}")
                print(f"   OrderID: {order['orderId']}")
                
                send_to_dlq(dlq_producer, order, str(e), 'ValueError')
                print(f"   ✅ Offset will be committed - moving to next message")
                print()
            
    except KeyboardInterrupt:
        print("\n\n⏹️  Consumer stopped by user")
    except Exception as e:
        print(f"\n❌ Consumer error: {e}")
        raise
    finally:
        print("\n🔄 Closing consumer and DLQ producer...")
        consumer.close()
        dlq_producer.flush()
        
        stats = calculator.get_stats()
        print("\n" + "=" * 60)
        print("📊 FINAL STATISTICS")
        print("=" * 60)
        print(f"Total Messages Received: {message_count}")
        print(f"Successfully Processed: {stats['count']}")
        print(f"Sent to DLQ: {dlq_count}")
        print(f"Total Revenue: ${stats['sum']:.2f}")
        print(f"Average Order Value: ${stats['average']:.2f}")
        print("=" * 60)
        print("\n✅ Consumer shut down gracefully")


if __name__ == '__main__':
    main()
