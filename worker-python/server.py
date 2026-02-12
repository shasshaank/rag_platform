from fastapi import FastAPI
from pydantic import BaseModel
from qdrant_client import QdrantClient
from langchain_community.embeddings import HuggingFaceEmbeddings

app = FastAPI()

client = QdrantClient(host="localhost", port=6333)
collection_name = "pdf_collection"

print("Loading model...")
embeddings = HuggingFaceEmbeddings(model_name="all-MiniLM-L6-v2")
print("Model loaded!")

class QueryRequest(BaseModel):
    question: str

@app.post("/chat")
def chat_with_pdf(req: QueryRequest):
    print(f"Received question: {req.question}")
    
    try:
        vector_math = embeddings.embed_query(req.question)
        
        search_result = client.query_points(
            collection_name=collection_name,
            query=vector_math,
            limit=3
        )
        context_text = "\n\n".join([hit.payload.get("text", "") for hit in search_result.points])
        
        return {
            "answer": "Here is the relevant info I found in your PDF:",
            "context": context_text
        }
        
    except Exception as e:
        print(f"ERROR: {e}")
        return {"error": str(e)}
