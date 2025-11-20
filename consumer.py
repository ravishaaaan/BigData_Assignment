"""
Kafka Avro Consumer for Order Messages.
Consumes orders from the 'orders' topic and calculates running average of prices.
Includes error simulation for testing fault tolerance.
"""

import json
import random
from confluent_kafka import DeserializingConsumer
from confluent_kafka.serialization import StringDeserializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer

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


def create_consumer():
    """
    Create and configure the Avro Consumer.
    
    Returns:
        DeserializingConsumer: Configured Kafka consumer with Avro deserialization
    """
    # Initialize Schema Registry client
    schema_registry_client = SchemaRegistryClient({
        'url': config.SCHEMA_REGISTRY_URL
    })
    
    # Load Avro schema
    avro_schema_str = load_avro_schema('order.avsc')
    
    # Create Avro deserializer for the value
    avro_deserializer = AvroDeserializer(
        schema_registry_client,
        avro_schema_str
    )
    
    # Consumer configuration
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
    # CHAOS MONKEY: Simulate errors for testing
    # 10% chance of error occurrence
    if random.random() < config.ERROR_SIMULATION_RATE:
        error_type = random.choice(['validation', 'connection'])
        
        if error_type == 'validation':
            # Simulate a data validation error (e.g., corrupt data, invalid price)
            # This is a non-retryable "poison pill" error
            raise ValueError(
                f"❌ SIMULATED ERROR: Invalid data for order {order['orderId']} - "
                f"Price validation failed: {order['price']}"
            )
        else:
            # Simulate a temporary connection error (e.g., database unavailable)
            # This is a retryable error
            raise ConnectionError(
                f"⚠️ SIMULATED ERROR: Temporary service unavailable while processing "
                f"order {order['orderId']}"
            )
    
    # Normal processing: extract price and update running average
    price = order['price']
    new_average = calculator.add_price(price)
    
    return new_average


def main():
    """
    Main consumer loop.
    Reads orders and calculates running average of prices.
    """
    print("🚀 Starting Kafka Avro Consumer...")
    print(f"📡 Connected to: {config.BOOTSTRAP_SERVERS}")
    print(f"📋 Schema Registry: {config.SCHEMA_REGISTRY_URL}")
    print(f"📥 Consuming from topic: {config.ORDERS_TOPIC}")
    print(f"👥 Consumer Group: {config.CONSUMER_GROUP_ID}")
    print("-" * 60)
    
    consumer = create_consumer()
    consumer.subscribe([config.ORDERS_TOPIC])
    
    calculator = RunningAverageCalculator()
    message_count = 0
    
    try:
        print("\n⏳ Waiting for messages... (Press Ctrl+C to stop)\n")
        
        while True:
            # Poll for messages (1 second timeout)
            msg = consumer.poll(timeout=1.0)
            
            if msg is None:
                # No message available, continue polling
                continue
            
            if msg.error():
                print(f"❌ Consumer error: {msg.error()}")
                continue
            
            # Successfully received a message
            message_count += 1
            order = msg.value()
            
            # Process message (includes error simulation)
            try:
                new_average = process_message(order, calculator)
                
                # Display message details
                print(f"📦 [Message #{message_count}] Consumed order:")
                print(f"   OrderID: {order['orderId']}")
                print(f"   Product: {order['product']}")
                print(f"   Price: ${order['price']:.2f}")
                print(f"   🔢 Running Average: ${new_average:.2f} (from {calculator.count} orders)")
                print(f"   📍 Partition: {msg.partition()} | Offset: {msg.offset()}")
                print()
                
            except (ValueError, ConnectionError) as e:
                # For now, just log the error - Steps 6 & 7 will handle these properly
                print(f"🔥 ERROR encountered:")
                print(f"   {str(e)}")
                print(f"   OrderID: {order['orderId']}")
                print(f"   Error Type: {type(e).__name__}")
                print(f"   ⚠️ Message processing failed - will handle in Steps 6 & 7")
                print()
            
    except KeyboardInterrupt:
        print("\n\n⏹️  Consumer stopped by user")
    except Exception as e:
        print(f"\n❌ Consumer error: {e}")
        raise
    finally:
        # Close consumer to commit final offsets
        print("\n🔄 Closing consumer...")
        consumer.close()
        
        # Print final statistics
        stats = calculator.get_stats()
        print("\n" + "=" * 60)
        print("📊 FINAL STATISTICS")
        print("=" * 60)
        print(f"Total Orders Processed: {stats['count']}")
        print(f"Total Revenue: ${stats['sum']:.2f}")
        print(f"Average Order Value: ${stats['average']:.2f}")
        print("=" * 60)
        print("\n✅ Consumer shut down gracefully")


if __name__ == '__main__':
    main()
