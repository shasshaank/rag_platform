# AuroraRAG 🌟

A full-stack, distributed **Retrieval-Augmented Generation (RAG)** platform designed to ingest PDF documents, generate vector embeddings, and provide intelligent, context-aware answers to user queries using advanced Large Language Models.

AuroraRAG is architected as a microservices application, prioritizing scalability, asynchronous processing, and an extremely polished user interface.

## 🚀 Key Features

- **Intelligent Document Chat:** Talk directly with your uploaded PDF documents. The AI intelligently retrieves relevant chunks of text and cites its sources.
- **Asynchronous Processing:** File uploads are instantly accepted by a Go-based API Gateway and queued into RabbitMQ, preventing frontend timeouts while a Python worker processes the heavy embedding tasks.
- **Conversational Memory:** Complete chat history persistence and conversation threading powered by Supabase.
- **Strict Grounding & Hallucination Mitigation:** The LLM is explicitly prompted to only answer using mathematical context retrieved from Qdrant, falling back to a general knowledge disclaimer gracefully when needed.
- **Secure Authentication:** Fully integrated signup, login, and Google OAuth using Supabase Auth.
- **Stunning UI/UX:** Built with Next.js 16, Tailwind CSS, and Shadcn UI, featuring loading skeletons, responsive design, dark mode, and dynamic document selection.

## 🏗️ Architecture & Tech Stack

AuroraRAG leverages a modern, distributed architecture to separate concerns and maximize performance.

### **The Stack**
- **Frontend:** Next.js 16 (React 19), TailwindCSS, Shadcn UI
- **API Gateway:** Go (Golang), Gin Framework
- **Message Broker:** RabbitMQ
- **Vector Database:** Qdrant
- **Backend Services:** Python, FastAPI
- **AI & ML Engine:** 
  - **LLM:** Meta Llama 3.3 70B (Powered by Groq for ultra-fast inference)
  - **Embeddings:** HuggingFace `all-MiniLM-L6-v2`
  - **Orchestration:** LangChain (PyPDFLoader, RecursiveCharacterTextSplitter)
- **Database & Auth:** Supabase (PostgreSQL with RLS)

### **System Flow**
1. **Upload:** User uploads a PDF via the Next.js client.
2. **Ingestion:** The Go Gateway receives the file, saves it, and publishes an upload event to RabbitMQ.
3. **Processing:** The Python Worker consumes the message, chunks the PDF, generates dense vectors via HuggingFace, and upserts them into Qdrant.
4. **Querying:** User asks a question. The FastAPI server embeds the query, searches Qdrant for semantic similarity, appends conversation history, and prompts the Llama 3.3 model.

---

## 🛠️ Local Development Setup

To run this project locally, you will need **Docker Desktop**, **Node.js**, **Python 3**, and **Go** installed on your machine.

### 1. Start Infrastructure (Docker)
Ensure your local instances of RabbitMQ and Qdrant are running:
```bash
# Example via Docker
docker run -d --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:3-management
docker run -d -p 6333:6333 -p 6334:6334 qdrant/qdrant
```

### 2. Environment Variables
Create a `.env.local` inside the `/frontend` directory:
```env
NEXT_PUBLIC_GATEWAY_URL=http://localhost:8081
NEXT_PUBLIC_CHAT_API_URL=http://localhost:8000
NEXT_PUBLIC_SUPABASE_URL=your_supabase_url
NEXT_PUBLIC_SUPABASE_ANON_KEY=your_supabase_anon_key
```

Create a `.env` in the root (for Python/Go backends):
```env
API_KEY=your_groq_api_key
SUPABASE_JWT_SECRET=your_supabase_jwt_secret
```

### 3. Run the API Gateway (Go)
Navigate to `gateway-go` and start the ingestion server:
```bash
cd gateway-go
go run main.go
# Runs on :8081
```

### 4. Run the Python Services (FastAPI + Worker)
Navigate to `worker-python`, setup your virtual environment, and install dependencies (`langchain`, `qdrant-client`, `groq`, `fastapi`, `pika`).

```bash
# Terminal 1: Start the Async Worker
python worker.py

# Terminal 2: Start the Chat/RAG Server
uvicorn server:app --reload --port 8000
```

### 5. Run the Frontend (Next.js)
Navigate to `frontend` and start the development server:
```bash
cd frontend
npm install
npm run dev
# Runs on :3000
```

---

## 💡 Future Roadmap

- [ ] Implement Hybrid Search (BM25 + Semantic Search)
- [ ] Multi-document cross-referencing capabilities
- [ ] Add explicit Agentic chunking methods to handle complex tabular PDF data
- [ ] Expand LLM choice selection (OpenAI, Anthropic) via user settings

---

*This project is designed to showcase an understanding of distributed microservices, advanced RAG principles, real-time message queuing, and enterprise-grade React development.*
