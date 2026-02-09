import pika
import json
import time


class RabbitMQProducer:
    def __init__(self):
        self.connection = None
        self.channel = None
        # Don't connect in __init__ - connect lazily when first used
    
    def connect(self):
        """Establish connection to RabbitMQ"""
        max_retries = 5
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                params = pika.URLParameters(
                    'amqp://guest:guest@rabbitmq:5672/'
                )
                self.connection = pika.BlockingConnection(params)
                self.channel = self.connection.channel()
                print("Producer connected to RabbitMQ")
                return True
            except Exception as e:
                retry_count += 1
                print(f"Producer waiting for RabbitMQ... (attempt {retry_count}/{max_retries}): {e}")
                time.sleep(2)
        
        print("Producer failed to connect to RabbitMQ")
        return False
    
    def ensure_connection(self):
        """Check if connection is alive, reconnect if needed"""
        if self.connection is None or self.connection.is_closed:
            print("Connection is closed, reconnecting...")
            return self.connect()
        if self.channel is None or self.channel.is_closed:
            print("Channel is closed, recreating channel...")
            try:
                self.channel = self.connection.channel()
                return True
            except:
                return self.connect()
        return True
    
    def publish(self, method, body):
        """Publish message to RabbitMQ with automatic reconnection"""
        if not self.ensure_connection():
            print("Failed to ensure connection, message not sent")
            return False
        
        try:
            properties = pika.BasicProperties(method)
            self.channel.basic_publish(
                exchange='',
                routing_key='main',
                body=json.dumps(body),
                properties=properties
            )
            print(f"Message published: {method}")
            return True
        except Exception as e:
            print(f"Error publishing message: {e}")
            # Try to reconnect and send again
            if self.connect():
                try:
                    properties = pika.BasicProperties(method)
                    self.channel.basic_publish(
                        exchange='',
                        routing_key='main',
                        body=json.dumps(body),
                        properties=properties
                    )
                    print(f"Message published after reconnection: {method}")
                    return True
                except Exception as e2:
                    print(f"Error publishing message after reconnection: {e2}")
                    return False
            return False


# Create singleton instance but DON'T connect yet
_producer = RabbitMQProducer()


def publish(method, body):
    """Public interface for publishing messages"""
    try:
        return _producer.publish(method, body)
    except Exception as e:
        print(f"Error in publish function: {e}")
        return False