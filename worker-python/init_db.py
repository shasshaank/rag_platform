from qdrant_client import QdrantClient
from qdrant_client.http import models

client = QdrantClient(host="localhost", port=6333)
collection_name = "pdf_collection"

print(f"Checking status of '{collection_name}'...")

if client.collection_exists(collection_name):
    client.delete_collection(collection_name)
    print(" -> Deleted old collection.")

client.create_collection(
    collection_name=collection_name,
    vectors_config=models.VectorParams(size=384, distance=models.Distance.COSINE),
)

print(f" -> [SUCCESS] Collection '{collection_name}' is created and ready!")