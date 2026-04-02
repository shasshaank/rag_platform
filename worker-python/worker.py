import sys
import pika
import json
import time
import os
import uuid
import traceback
import boto3
from urllib.parse import urlparse
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

AWS_REGION = os.getenv("AWS_REGION", "us-east-1")

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

def download_from_s3(s3_uri: str, local_path: str):
    print(f" [S3] Downloading {s3_uri} to {local_path}...")
    s3_client = boto3.client('s3', region_name=AWS_REGION)
    parsed = urlparse(s3_uri)
    bucket = parsed.netloc
    key = parsed.path.lstrip('/')
    s3_client.download_file(bucket, key, local_path)
    print(" [S3] Download complete.")

def process_pdf(file_path: str, job_id: str) -> str:
    print(f" [O] Processing file: {file_path}")
    
    local_path = file_path
    is_s3 = file_path.startswith("s3://")
    
    if is_s3:
        filename = os.path.basename(file_path)
        os.makedirs("temp-downloads", exist_ok=True)
        local_path = os.path.join("temp-downloads", filename)
        download_from_s3(file_path, local_path)

    if not os.path.exists(local_path):
        raise FileNotFoundError(f"File not found at {local_path}")

    ensure_collection_exists()

    doc_id = job_id
    filename = os.path.basename(file_path)

    loader = PyPDFLoader(local_path)
    try:
        pages = loader.load()
    except Exception as e:
        if is_s3 and os.path.exists(local_path):
            os.remove(local_path)
        raise e

    print(f"     -> Loaded {len(pages)} pages")

    text_splitter = RecursiveCharacterTextSplitter(chunk_size=500, chunk_overlap=50)
    chunks = text_splitter.split_documents(pages)
    print(f"     -> Split into {len(chunks)} text chunks")

    # Filter out empty chunks to avoid embedding crashes
    valid_chunks = []
    texts_to_embed = []
    for chunk in chunks:
        text = str(chunk.page_content).strip() if chunk.page_content else ""
        if text:
            valid_chunks.append((chunk, text))
            texts_to_embed.append(text)

    if not texts_to_embed:
        if is_s3 and os.path.exists(local_path):
            os.remove(local_path)
        raise ValueError(f"No readable text found in {local_path}. Cannot store in database.")

    print(f"     -> Batch embedding {len(texts_to_embed)} chunks... (This will be much faster!)")
    # Batch embed ALL chunks at once (massively speeds up processing)
    vectors = embed_model.embed_documents(texts_to_embed)

    points = []
    for i, (chunk_tup, vector) in enumerate(zip(valid_chunks, vectors)):
        chunk, text = chunk_tup
        payload = {
            "text": text,
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

    batch_size = 100
    for i in range(0, len(points), batch_size):
        qdrant_client.upsert(
            collection_name=COLLECTION_NAME, 
            points=points[i:i+batch_size]
        )
    print(f" [V] Success! Stored {len(points)} vectors in Qdrant for doc_id={doc_id}.")
    
    if is_s3 and os.path.exists(local_path):
        os.remove(local_path)
        print(f"     -> Cleaned up temporary file {local_path}")
        
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
        traceback.print_exc()
        try:
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
        except Exception as nack_err:
            print(f"Failed to nack message: {nack_err}")

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