from fastapi import FastAPI
from pydantic import BaseModel
from qdrant_client import QdrantClient
from qdrant_client.http import models
from langchain_community.embeddings import HuggingFaceEmbeddings
from groq import Groq
import os
from dotenv import load_dotenv
from fastapi.middleware.cors import CORSMiddleware


app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


load_dotenv()
api_key=os.getenv("API_KEY")
QDRANT_HOST = os.getenv("QDRANT_HOST", "localhost")
QDRANT_PORT = int(os.getenv("QDRANT_PORT", "6333"))
collection_name = os.getenv("QDRANT_COLLECTION", "pdf_collection")

client = QdrantClient(host=QDRANT_HOST, port=QDRANT_PORT)

groq_client = Groq(api_key=api_key) 

print("Loading embedding model...")
embeddings = HuggingFaceEmbeddings(model_name="all-MiniLM-L6-v2")
print("Model loaded!")

class QueryRequest(BaseModel):
    question: str
    doc_ids: list[str]
    chat_history: list[dict] = []

@app.post("/chat")
def chat_with_pdf(req: QueryRequest):
    print(f"Received question: {req.question} (doc_ids={req.doc_ids})")

    try:
        vector_math = embeddings.embed_query(req.question)

        filter_ = models.Filter(
            must=[
                models.FieldCondition(
                    key="doc_id",
                    match=models.MatchAny(any=req.doc_ids),
                )
            ]
        ) if req.doc_ids else None

        search_result = client.query_points(
            collection_name=collection_name,
            query=vector_math,
            query_filter=filter_,
            limit=8,
        )

        # Build numbered context blocks so the model can cite [1], [2], ...
        context_blocks = []
        citations = []

        for idx, hit in enumerate(search_result.points, start=1):
            payload = hit.payload or {}
            text = payload.get("text", "")

            context_blocks.append(
                f"[{idx}] (file={payload.get('filename')}, page={payload.get('page')}, chunk={payload.get('chunk_id')})\n{text}"
            )

            citations.append({
                "idx": idx,
                "score": getattr(hit, "score", None),
                "doc_id": payload.get("doc_id"),
                "filename": payload.get("filename"),
                "page": payload.get("page"),
                "chunk_id": payload.get("chunk_id"),
                "text_preview": (text[:240] + "...") if text else "",
                "text": text,
            })

        # IMPORTANT: handle empty retrieval so we never return None
        if not context_blocks:
            return {
                "answer": "I don't know based on the provided documents.",
                "citations": [],
            }

        context_text = "\n\n".join(context_blocks)

        prompt_messages = [
            {
                "role": "system",
                "content": f"""
You are an assistant that answers ONLY using the provided context.
If the answer isn't supported by the context, say: "I don't know based on the provided documents."
When you use information, cite sources like [1] or [2].

CONTEXT:
{context_text}
"""
            }
        ]

        # Append previous chat history
        for msg in req.chat_history[-6:]:  # Keep last 6 messages
            prompt_messages.append({"role": msg.get("role", "user"), "content": msg.get("content", "")})

        # Append the current prompt
        prompt_messages.append({"role": "user", "content": req.question})

        chat_completion = groq_client.chat.completions.create(
            messages=prompt_messages,
            model="llama-3.3-70b-versatile",
        )

        final_answer = chat_completion.choices[0].message.content

        return {
            "answer": final_answer,
            "citations": citations,
            "retrieval_count":len(citations),
            "doc_ids":req.doc_ids,
        }

    except Exception as e:
        print(f"ERROR: {e}")
        return {"error": str(e)}

@app.delete("/document/{doc_id}")
def delete_document(doc_id: str):
    print(f"Deleting document vectors for doc_id: {doc_id}")
    try:
        client.delete(
            collection_name=collection_name,
            points_selector=models.FilterSelector(
                filter=models.Filter(
                    must=[
                        models.FieldCondition(
                            key="doc_id",
                            match=models.MatchValue(value=doc_id),
                        )
                    ]
                )
            )
        )
        return {"status": "success"}
    except Exception as e:
        print(f"ERROR deleting document {doc_id}: {e}")
        return {"error": str(e)}

