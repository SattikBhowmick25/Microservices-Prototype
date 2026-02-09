import pika, json
import time
from main import Product, db

params = pika.URLParameters('amqp://guest:guest@host.docker.internal:5672/')

# Retry connection logic
max_retries = 30
retry_count = 0
connection = None

while retry_count < max_retries:
    try:
        connection = pika.BlockingConnection(params)
        channel = connection.channel()
        print("Consumer connected to RabbitMQ!")
        break
    except:
        retry_count += 1
        print(f"Consumer waiting for RabbitMQ... (attempt {retry_count}/{max_retries})")
        time.sleep(2)

if connection is None:
    print("Consumer failed to connect to RabbitMQ")
    exit(1)

channel = connection.channel()
channel.queue_declare(queue='main')

def callback(ch, method, properties, body):
    print('Received in main')
    data = json.loads(body)
    print(data)
    
    if properties.content_type == 'product_created':
        product = Product(id=data['id'], title=data['title'], image=data['image'])
        db.session.add(product)
        db.session.commit()
        print('Product Created')
    elif properties.content_type == 'product_updated':
        product = Product.query.get(data['id'])
        product.title = data['title']
        product.image = data['image']
        db.session.commit()
        print('Product Updated')
    elif properties.content_type == 'product_deleted':
        product = Product.query.get(data)
        db.session.delete(product)
        db.session.commit()
        print('Product Deleted')

channel.basic_consume(queue='main', on_message_callback=callback, auto_ack=True)
print('Started Consuming')
channel.start_consuming()
channel.close()