'use client';

import { useState } from 'react';
import { FileText, Loader2, Upload } from 'lucide-react';
import { ChatWindow } from '@/components/ChatWindow';
import { RightPanel } from '@/components/RightPanel';
import type { Message } from '@/lib/types';
import { getPublicEnv } from '@/lib/env';

function cx(...classes: Array<string | false | undefined | null>) {
  return classes.filter(Boolean).join(' ');
}

export default function Home() {
  const { gateway, chatApi } = getPublicEnv();

  const [messages, setMessages] = useState<Message[]>([]);
  const [input, setInput] = useState('');
  const [isUploading, setIsUploading] = useState(false);
  const [isTyping, setIsTyping] = useState(false);
  const [docId, setDocId] = useState<string | null>(null);

  const handleFileUpload = async (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0];
    if (!file) return;

    setIsUploading(true);
    const formData = new FormData();
    formData.append('file', file);

    try {
      if (!gateway) {
        alert('Missing NEXT_PUBLIC_GATEWAY_URL in environment.');
        return;
      }

      const res = await fetch(`${gateway}/upload`, {
        method: 'POST',
        body: formData,
      });

      const data = await res.json().catch(() => null);

      if (res.ok && data?.job_id) {
        setDocId(data.job_id);
        setMessages([]);
        alert(`Uploaded. doc_id = ${data.job_id}`);
      } else {
        alert(`Failed to upload file.${data?.error ? ` ${data.error}` : ''}`);
      }
    } catch (err) {
      console.error(err);
      alert('Error uploading file. Is the Go server running?');
    } finally {
      setIsUploading(false);
      e.target.value = '';
    }
  };

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!input.trim() || isTyping) return;

    const userMessage = input.trim();
    setInput('');
    setMessages((prev) => [...prev, { role: 'user', content: userMessage }]);
    setIsTyping(true);

    try {
      if (!docId) {
        setMessages((prev) => [
          ...prev,
          { role: 'assistant', content: 'Upload a PDF first, then ask questions.' },
        ]);
        return;
      }
      if (!chatApi) {
        setMessages((prev) => [
          ...prev,
          { role: 'assistant', content: 'Missing NEXT_PUBLIC_CHAT_API_URL in environment.' },
        ]);
        return;
      }

      const res = await fetch(`${chatApi}/chat`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ question: userMessage, doc_id: docId }),
      });

      const data = await res.json().catch(() => ({} as any));

      setMessages((prev) => [
        ...prev,
        {
          role: 'assistant',
          content: data.answer || data.error || 'Unknown error occurred.',
          citations: Array.isArray(data.citations) ? data.citations : [],
        },
      ]);
    } catch (err) {
      console.error(err);
      setMessages((prev) => [
        ...prev,
        { role: 'assistant', content: 'Network error. Is the Python server running?' },
      ]);
    } finally {
      setIsTyping(false);
    }
  };

  return (
    <div className="h-screen bg-gradient-to-b from-slate-50 to-slate-100 text-slate-900">
      {/* Top bar */}
      <div className="border-b bg-white/70 backdrop-blur">
        <div className="max-w-6xl mx-auto px-4 py-3 flex items-center justify-between gap-4">
          <div className="flex items-center gap-2">
            <FileText className="text-blue-600" />
            <div>
              <div className="text-lg font-semibold leading-tight">DocuChat AI</div>
              <div className="text-xs text-slate-500">
                {docId ? (
                  <>
                    Document loaded: <span className="font-mono">{docId}</span>
                  </>
                ) : (
                  'Upload a PDF to start'
                )}
              </div>
            </div>
          </div>

          <div className="flex items-center gap-3">
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
              className={cx(
                'inline-flex items-center gap-2 px-3 py-2 rounded-lg border bg-white hover:bg-slate-50 cursor-pointer shadow-sm',
                isUploading && 'opacity-60 cursor-not-allowed'
              )}
            >
              {isUploading ? <Loader2 className="w-4 h-4 animate-spin" /> : <Upload className="w-4 h-4" />}
              <span className="text-sm">{isUploading ? 'Uploading…' : 'Upload PDF'}</span>
            </label>
          </div>
        </div>
      </div>

      {/* Main */}
      <div className="max-w-6xl mx-auto h-[calc(100vh-57px)] grid grid-cols-1 md:grid-cols-[1.6fr_1fr] gap-4 p-4">
        <ChatWindow
          docId={docId}
          messages={messages}
          input={input}
          setInput={setInput}
          isTyping={isTyping}
          onSubmit={handleSubmit}
        />
        <RightPanel
          docId={docId}
          messages={messages}
          gatewayOk={Boolean(gateway)}
          chatOk={Boolean(chatApi)}
        />
      </div>
    </div>
  );
}