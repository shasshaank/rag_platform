import sys
import pika
import json
import time
import os
import uuid
from qdrant_client import QdrantClient
from qdrant_client.http import models
from langchain_community.document_loaders import PyPDFLoader
from langchain_text_splitters import RecursiveCharacterTextSplitter
from langchain_community.embeddings import HuggingFaceEmbeddings

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
QUEUE_NAME = os.getenv("RAG_QUEUE_NAME", "rag_jobs")

QDRANT_HOST = os.getenv("QDRANT_HOST", "localhost")
QDRANT_PORT = int(os.getenv("QDRANT_PORT", "6333"))
COLLECTION_NAME = os.getenv("QDRANT_COLLECTION", "pdf_collection")

qdrant_client = QdrantClient(host=QDRANT_HOST, port=QDRANT_PORT)
print(" [i] Loading AI model... (This happens once)")
embed_model = HuggingFaceEmbeddings(model_name="all-MiniLM-L6-v2")
print(" [i] Model loaded!")


def ensure_collection_exists():
    if qdrant_client.collection_exists(COLLECTION_NAME):
        return
    print(f"[i] Creating collection '{COLLECTION_NAME}' ... ")
    qdrant_client.create_collection(
        collection_name = COLLECTION_NAME,
        vectors_config = models.VectorParams(size=384,distance=models.Distance.COSINE)
    )
    print(f"     -> Created Collection: {COLLECTION_NAME} ")

def process_pdf(file_path: str, job_id: str) -> str:
    print(f" [O] Processing file: {file_path}")

    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found at {file_path}")

    ensure_collection_exists()

    doc_id = job_id
    filename = os.path.basename(file_path)

    loader = PyPDFLoader(file_path)
    pages = loader.load()
    print(f"     -> Loaded {len(pages)} pages")

    text_splitter = RecursiveCharacterTextSplitter(chunk_size=500, chunk_overlap=50)
    chunks = text_splitter.split_documents(pages)
    print(f"     -> Split into {len(chunks)} text chunks")

    points = []
    for i, chunk in enumerate(chunks):
        vector = embed_model.embed_query(chunk.page_content)

        payload = {
            "text": chunk.page_content,
            "source": file_path,
            "filename": filename,
            "doc_id": doc_id,
            "chunk_id": i,
            "page": chunk.metadata.get("page"),
        }

        points.append(
            models.PointStruct(
                id=str(uuid.uuid4()),
                vector=vector,
                payload=payload,
            )
        )

    if not points:
        raise ValueError(f"No readable text found in {file_path}. Cannot store in database.")

    qdrant_client.upsert(collection_name=COLLECTION_NAME, points=points)
    print(f" [V] Success! Stored {len(points)} vectors in Qdrant for doc_id={doc_id}.")
    return doc_id



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


def callback(ch, method, properties, body):
    try:
        message = json.loads(body)
        file_path = message.get('file_path')
        job_id = message.get('job_id')
        
        print(f"\n [x] Received Job ID: {job_id}")
        doc_id = process_pdf(file_path, job_id)

        print(f" [x] Done (doc_id={doc_id})")

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