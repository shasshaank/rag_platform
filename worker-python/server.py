from fastapi import FastAPI, Request
from pydantic import BaseModel
from qdrant_client import QdrantClient
from qdrant_client.http import models
from langchain_community.embeddings import HuggingFaceEmbeddings
from groq import Groq
import os
import re
import jwt
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
SUPABASE_JWT_SECRET = os.getenv("SUPABASE_JWT_SECRET", "")

client = QdrantClient(host=QDRANT_HOST, port=QDRANT_PORT)

groq_client = Groq(api_key=api_key) 

print("Loading embedding model...")
embeddings = HuggingFaceEmbeddings(model_name="all-MiniLM-L6-v2")
print("Model loaded!")

def get_user_from_token(request: Request) -> str | None:
    """Extract and verify Supabase JWT from Authorization header."""
    auth_header = request.headers.get("authorization", "")
    if not auth_header.startswith("Bearer "):
        return None
    token = auth_header[7:]
    try:
        payload = jwt.decode(
            token,
            SUPABASE_JWT_SECRET,
            algorithms=["HS256"],
            audience="authenticated",
        )
        return payload.get("sub")  # sub = user_id
    except jwt.ExpiredSignatureError:
        print("[AUTH] Token expired")
        return None
    except jwt.InvalidTokenError as e:
        print(f"[AUTH] Invalid token: {e}")
        return None

class QueryRequest(BaseModel):
    question: str
    doc_ids: list[str]
    chat_history: list[dict] = []

@app.post("/chat")
def chat_with_pdf(req: QueryRequest):
    print(f"\n{'='*60}")
    print(f"[DEBUG] Received question: {req.question}")
    print(f"[DEBUG] doc_ids from frontend: {req.doc_ids}")
    print(f"[DEBUG] doc_ids types: {[type(d).__name__ for d in req.doc_ids]}")
    print(f"[DEBUG] chat_history length: {len(req.chat_history)}")

    try:
        vector_math = embeddings.embed_query(req.question)

        any_doc_ids_str = []
        any_doc_ids_int = []
        if req.doc_ids:
            for d in req.doc_ids:
                any_doc_ids_str.append(str(d))
                if isinstance(d, str) and d.isdigit():
                    any_doc_ids_int.append(int(d))

        print(f"[DEBUG] any_doc_ids_str: {any_doc_ids_str}")
        print(f"[DEBUG] any_doc_ids_int: {any_doc_ids_int}")

        filter_ = None
        if req.doc_ids:
            conditions = []
            if any_doc_ids_str:
                conditions.append(models.FieldCondition(key="doc_id", match=models.MatchAny(any=any_doc_ids_str)))
            if any_doc_ids_int:
                conditions.append(models.FieldCondition(key="doc_id", match=models.MatchAny(any=any_doc_ids_int)))
            
            filter_ = models.Filter(should=conditions)
            print(f"[DEBUG] Filter constructed with {len(conditions)} conditions")
        else:
            print(f"[DEBUG] WARNING: No doc_ids provided, no filter applied!")

        search_result = client.query_points(
            collection_name=collection_name,
            query=vector_math,
            query_filter=filter_,
            limit=8,
        )

        print(f"[DEBUG] Qdrant returned {len(search_result.points)} results")

        # Build numbered context blocks so the model can cite [1], [2], ...
        context_blocks = []
        citations = []

        for idx, hit in enumerate(search_result.points, start=1):
            payload = hit.payload or {}
            text = payload.get("text", "")
            score = getattr(hit, "score", None)
            
            print(f"[DEBUG]   Hit #{idx}: score={score}, doc_id={payload.get('doc_id')}, text[:80]={text[:80]}")

            context_blocks.append(
                f"[{idx}] (file={payload.get('filename')}, page={payload.get('page')}, chunk={payload.get('chunk_id')})\n{text}"
            )

            citations.append({
                "idx": idx,
                "score": score,
                "doc_id": payload.get("doc_id"),
                "filename": payload.get("filename"),
                "page": payload.get("page"),
                "chunk_id": payload.get("chunk_id"),
                "text_preview": (text[:240] + "...") if text else "",
                "text": text,
            })

        # IMPORTANT: handle empty retrieval so the LLM can fallback to general knowledge
        if not context_blocks:
            context_text = "No relevant documents found."
            print(f"[DEBUG] WARNING: No context blocks! Retrieval returned empty results.")
        else:
            context_text = "\n\n".join(context_blocks)
            print(f"[DEBUG] Context text length: {len(context_text)} chars")

        prompt_messages = [
            {
                "role": "system",
                "content": f"""
You are an intelligent assistant. You have been provided with CONTEXT retrieved from the user's uploaded documents.

RULES:
1. If the CONTEXT contains information relevant to the user's question, answer USING ONLY that information and cite your sources like [1], [2], etc.
2. NOTE: The filename, page number, and chunk metadata provided in the parentheses (e.g., (file=...)) ALSO count as valid CONTEXT. You may use the filename to infer the title, author, or subject of the document to answer the user's question.
3. If the CONTEXT (including the metadata/filename) does NOT contain relevant information, or if no CONTEXT is provided, you MAY answer from your general knowledge BUT you MUST start your response with exactly this line:
   "⚠️ This answer is based on general knowledge, not your uploaded documents."
   Then provide your answer below that disclaimer.
4. Do NOT mix document citations with general knowledge. Either cite documents OR give the disclaimer — never both.

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

        print(f"[DEBUG] Sending {len(prompt_messages)} messages to LLM")
        print(f"[DEBUG] System prompt length: {len(prompt_messages[0]['content'])} chars")

        chat_completion = groq_client.chat.completions.create(
            messages=prompt_messages,
            model="llama-3.3-70b-versatile",
        )

        final_answer = chat_completion.choices[0].message.content
        print(f"[DEBUG] LLM response length: {len(final_answer)} chars")
        print(f"[DEBUG] LLM response preview: {final_answer[:200]}")

        # Determine which citations the LLM actually used in its answer
        used_indices = set(int(m) for m in re.findall(r'\[(\d+)\]', final_answer))
        used_citations = [c for c in citations if c["idx"] in used_indices]
        
        # Determine source type
        is_general_knowledge = len(used_citations) == 0
        source_type = "general_knowledge" if is_general_knowledge else "document"
        
        print(f"[DEBUG] Used citation indices: {used_indices}")
        print(f"[DEBUG] Source type: {source_type}")
        print(f"{'='*60}\n")

        return {
            "answer": final_answer,
            "citations": used_citations,
            "retrieval_count": len(used_citations),
            "source_type": source_type,
            "doc_ids": req.doc_ids,
        }

    except Exception as e:
        print(f"ERROR: {e}")
        import traceback
        traceback.print_exc()
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

@app.get("/status/{doc_id}")
def check_document_status(doc_id: str):
    """Check if a document has been processed and indexed in Qdrant."""
    try:
        results, _ = client.scroll(
            collection_name=collection_name,
            scroll_filter=models.Filter(
                must=[
                    models.FieldCondition(
                        key="doc_id",
                        match=models.MatchValue(value=doc_id),
                    )
                ]
            ),
            limit=1,
            with_payload=True,
            with_vectors=False,
        )
        
        if results:
            return {
                "status": "ready",
                "doc_id": doc_id,
                "filename": results[0].payload.get("filename", ""),
            }
        else:
            return {
                "status": "processing",
                "doc_id": doc_id,
            }
    except Exception as e:
        print(f"ERROR checking status for {doc_id}: {e}")
        return {
            "status": "processing",
            "doc_id": doc_id,
        }
