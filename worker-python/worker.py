import pika
import json
import time
import os

RABBITMQ_HOST = 'localhost'
QUEUE_NAME = 'rag_jobs'

def connect_to_rabbitmq():
    while True:
        try:
            connection = pika.BlockingConnection(
                pika.ConnectionParameters(host=RABBITMQ_HOST)
            )
            return connection
        except pika.exceptions.AMQPConnectionError:
            print(f"Waiting for RabbitMQ at {RABBITMQ_HOST}...")
            time.sleep(5)

def process_pdf(file_path):
    print(f" [O] Processing file: {file_path}")
    
    
    time.sleep(2) 
    
    if os.path.exists(file_path):
        file_size = os.path.getsize(file_path)
        print(f" [V] File found! Size: {file_size} bytes")
    else:
        print(f" [X] Error: File not found at {file_path}")

def callback(ch, method, properties, body):
    try:
        message = json.loads(body)
        file_path = message.get('file_path')
        job_id = message.get('job_id')
        
        print(f"\n [x] Received Job ID: {job_id}")
        
        process_pdf(file_path)
        
        print(" [x] Done")

        ch.basic_ack(delivery_tag=method.delivery_tag)
        
    except Exception as e:
        print(f"Error processing message: {e}")

def main():
    print(" [*] Worker started. Connecting to RabbitMQ...")
    connection = connect_to_rabbitmq()
    channel = connection.channel()

    
    channel.queue_declare(queue=QUEUE_NAME, durable=True)

    
    channel.basic_qos(prefetch_count=1)

    channel.basic_consume(queue=QUEUE_NAME, on_message_callback=callback)

    print(' [*] Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)