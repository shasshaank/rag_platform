import pika
import json
import time
import os

# ---------------------------------------------------------
# CONFIGURATION
# ---------------------------------------------------------
RABBITMQ_HOST = 'localhost'
QUEUE_NAME = 'rag_jobs'

def connect_to_rabbitmq():
    """
    Connects to RabbitMQ with a retry mechanism.
    Sometimes Docker takes a few seconds to spin up, so we wait.
    """
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
    """
    Placeholder for the heavy AI logic (Phase 2).
    For now, we just verify the file exists.
    """
    print(f" [O] Processing file: {file_path}")
    
    # Simulate heavy work (e.g., reading PDF, generating embeddings)
    time.sleep(2) 
    
    if os.path.exists(file_path):
        file_size = os.path.getsize(file_path)
        print(f" [V] File found! Size: {file_size} bytes")
    else:
        print(f" [X] Error: File not found at {file_path}")

def callback(ch, method, properties, body):
    """
    This function triggers automatically whenever a message arrives.
    """
    try:
        # 1. Parse the JSON message from Go
        message = json.loads(body)
        file_path = message.get('file_path')
        job_id = message.get('job_id')
        
        print(f"\n [x] Received Job ID: {job_id}")
        
        # 2. Run the processing logic
        process_pdf(file_path)
        
        print(" [x] Done")

        # 3. Acknowledge the message (Tell RabbitMQ it's safe to delete)
        ch.basic_ack(delivery_tag=method.delivery_tag)
        
    except Exception as e:
        print(f"Error processing message: {e}")
        # Note: In a real app, you might send this to a 'Dead Letter Queue'

def main():
    print(" [*] Worker started. Connecting to RabbitMQ...")
    connection = connect_to_rabbitmq()
    channel = connection.channel()

    # Declare the queue (idempotent: works even if Go already created it)
    channel.queue_declare(queue=QUEUE_NAME, durable=True)

    # Tell RabbitMQ not to give this worker more than 1 message at a time
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