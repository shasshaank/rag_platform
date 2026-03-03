from fastapi import FastAPI
from pydantic import BaseModel
from qdrant_client import QdrantClient
from langchain_community.embeddings import HuggingFaceEmbeddings
from groq import Groq
import os
from dotenv import load_dotenv



app = FastAPI()
load_dotenv()
api_key=os.getenv("API_KEY")
client = QdrantClient(host="localhost", port=6333)
collection_name = "pdf_collection"

groq_client = Groq(api_key=api_key) 

print("Loading embedding model...")
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
        
        prompt = f"""
        You are an intelligent assistant. Use the following context retrieved from a document to answer the user's question. 
        If the answer is not in the context, say "I don't know based on the provided document." Do not make up information.
        
        Context from Document:
        {context_text}
        
        User Question:
        {req.question}
        """
        chat_completion = groq_client.chat.completions.create(
            messages=[{"role": "user", "content": prompt}],
            model="llama-3.3-70b-versatile", 
        )
        
        final_answer = chat_completion.choices[0].message.content
        
        return {
            "answer": final_answer,
        }
        
    except Exception as e:
        print(f"ERROR: {e}")
        return {"error": str(e)}

#