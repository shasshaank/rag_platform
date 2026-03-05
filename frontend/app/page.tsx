'use client';

import { useState } from 'react';
import { Send, FileText, Loader2, Upload } from 'lucide-react';

export default function Home() {
  const [messages, setMessages] = useState<{ role: 'user' | 'assistant', content: string }[]>([]);
  const [input, setInput] = useState('');
  const [isUploading, setIsUploading] = useState(false);
  const [isTyping, setIsTyping] = useState(false);

  // 1. FILE UPLOAD HANDLER
  const handleFileUpload = async (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0];
    if (!file) return;

    setIsUploading(true);
    const formData = new FormData();
    formData.append('file', file);

    try {
      // Connects to your GO GATEWAY
      const res = await fetch('http://localhost:8080/upload', {
        method: 'POST',
        body: formData,
      });
      
      if (res.ok) {
        alert('File uploaded successfully! Processing started in the background.');
      } else {
        alert('Failed to upload file.');
      }
    } catch (error) {
      console.error(error);
      alert('Error uploading file. Is the Go server running?');
    } finally {
      setIsUploading(false);
      // Reset input so you can upload the same file again if needed
      e.target.value = '';
    }
  };

  // 2. CHAT MESSAGE HANDLER
  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!input.trim() || isTyping) return;

    const userMessage = input;
    setInput('');
    setMessages((prev) => [...prev, { role: 'user', content: userMessage }]);
    setIsTyping(true);

    try {
      // Connects to your PYTHON FASTAPI SERVER
      const res = await fetch('http://localhost:8000/chat', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({ question: userMessage }),
      });

      const data = await res.json();
      
      setMessages((prev) => [
        ...prev,
        { role: 'assistant', content: data.answer || data.error || 'Unknown error occurred.' },
      ]);
    } catch (error) {
      setMessages((prev) => [
        ...prev,
        { role: 'assistant', content: 'Network error. Is the Python server running?' },
      ]);
    } finally {
      setIsTyping(false);
    }
  };

  // 3. THE UI RENDER
  return (
    <div className="flex flex-col h-screen max-w-4xl mx-auto p-4">
      {/* Header */}
      <header className="flex items-center justify-between py-4 border-b">
        <h1 className="text-2xl font-bold flex items-center gap-2">
          <FileText className="text-blue-500" />
          DocuChat AI
        </h1>
        
        {/* Upload Button */}
        <div>
          <input
            type="file"
            id="file-upload"
            className="hidden"
            accept=".pdf"
            onChange={handleFileUpload}
            disabled={isUploading}
          />
          <label
            htmlFor="file-upload"
            className={`flex items-center gap-2 px-4 py-2 bg-slate-100 hover:bg-slate-200 text-slate-700 rounded-lg cursor-pointer transition-colors ${isUploading ? 'opacity-50 cursor-not-allowed' : ''}`}
          >
            {isUploading ? <Loader2 className="animate-spin w-4 h-4" /> : <Upload className="w-4 h-4" />}
            {isUploading ? 'Uploading...' : 'Upload PDF'}
          </label>
        </div>
      </header>

      {/* Chat History Area */}
      <main className="flex-1 overflow-y-auto py-6 space-y-4">
        {messages.length === 0 ? (
          <div className="text-center text-slate-500 mt-20">
            <p>Upload a PDF and start asking questions.</p>
          </div>
        ) : (
          messages.map((msg, i) => (
            <div
              key={i}
              className={`flex ${msg.role === 'user' ? 'justify-end' : 'justify-start'}`}
            >
              <div
                className={`max-w-[80%] rounded-2xl px-5 py-3 ${
                  msg.role === 'user'
                    ? 'bg-blue-500 text-white'
                    : 'bg-white border text-slate-800 shadow-sm'
                }`}
              >
                {msg.content}
              </div>
            </div>
          ))
        )}
        
        {isTyping && (
           <div className="flex justify-start">
             <div className="bg-white border text-slate-500 shadow-sm rounded-2xl px-5 py-3 flex gap-1">
               <span className="animate-bounce">.</span>
               <span className="animate-bounce delay-100">.</span>
               <span className="animate-bounce delay-200">.</span>
             </div>
           </div>
        )}
      </main>

      {/* Input Area */}
      <footer className="pt-4 border-t">
        <form onSubmit={handleSubmit} className="flex gap-2">
          <input
            type="text"
            value={input}
            onChange={(e) => setInput(e.target.value)}
            placeholder="Ask about your document..."
            className="flex-1 rounded-xl border p-4 shadow-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
            disabled={isTyping}
          />
          <button
            type="submit"
            disabled={isTyping || !input.trim()}
            className="bg-blue-500 hover:bg-blue-600 text-white p-4 rounded-xl disabled:opacity-50 transition-colors flex items-center justify-center"
          >
            <Send className="w-5 h-5" />
          </button>
        </form>
      </footer>
    </div>
  );
}