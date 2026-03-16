'use client';

import { Send } from 'lucide-react';
import type { Message } from '@/lib/types';
import { MessageBubble } from '@/components/MessageBubble';

function cx(...classes: Array<string | false | undefined | null>) {
  return classes.filter(Boolean).join(' ');
}

export function ChatWindow({
  docId,
  messages,
  input,
  setInput,
  isTyping,
  onSubmit,
}: {
  docId: string | null;
  messages: Message[];
  input: string;
  setInput: (v: string) => void;
  isTyping: boolean;
  onSubmit: (e: React.FormEvent) => void;
}) {
  return (
    <div className="bg-white border rounded-2xl flex flex-col min-h-0 shadow-sm">
      <div className="px-4 py-3 border-b">
        <div className="text-sm font-semibold">Chat</div>
        <div className="text-xs text-slate-500">
          Ask questions about the uploaded PDF. Answers include sources.
        </div>
      </div>

      <div className="flex-1 min-h-0 overflow-y-auto px-4 py-4 space-y-3">
        {messages.length === 0 ? (
          <div className="text-sm text-slate-500">
            Upload a PDF, then ask: “Summarize this document” or “What are the key points?”
          </div>
        ) : (
          messages.map((m, i) => <MessageBubble key={i} msg={m} />)
        )}

        {isTyping && (
          <div className="flex justify-start">
            <div className="bg-slate-50 border text-slate-500 rounded-2xl px-4 py-3 text-sm">
              Thinking…
            </div>
          </div>
        )}
      </div>

      <div className="border-t p-3">
        <form onSubmit={onSubmit} className="flex gap-2">
          <input
            type="text"
            value={input}
            onChange={(e) => setInput(e.target.value)}
            placeholder={docId ? 'Ask about your document…' : 'Upload a PDF to begin…'}
            className="flex-1 rounded-xl border px-4 py-3 text-sm shadow-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
            disabled={isTyping}
          />
          <button
            type="submit"
            disabled={isTyping || !input.trim()}
            className={cx(
              'px-4 py-3 rounded-xl text-white transition-colors inline-flex items-center justify-center shadow-sm',
              'bg-blue-600 hover:bg-blue-700 disabled:opacity-50'
            )}
            title="Send"
          >
            <Send className="w-4 h-4" />
          </button>
        </form>

        <div className="mt-2 text-[11px] text-slate-500">
          Tip: Ask for a summary, key points, or “cite the page/section”.
        </div>
      </div>
    </div>
  );
}