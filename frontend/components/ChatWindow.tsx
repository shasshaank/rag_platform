'use client';

import { Send, Sparkles } from 'lucide-react';
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
    <section className="rounded-2xl border border-slate-200/80 bg-white shadow-sm flex flex-col min-h-0">
      <div className="px-4 py-3 border-b border-slate-200/80">
        <div className="text-sm font-semibold">Chat</div>
        <div className="text-xs text-slate-500">Ask questions about your PDF and get cited answers.</div>
      </div>

      <div className="flex-1 min-h-0 overflow-y-auto px-4 py-4 space-y-3">
        {messages.length === 0 ? (
          <div className="rounded-2xl border border-dashed border-slate-300 bg-slate-50/70 p-5 text-sm text-slate-600">
            <div className="font-medium text-slate-700 mb-1 inline-flex items-center gap-2">
              <Sparkles className="w-4 h-4 text-violet-600" />
              Try a starter prompt
            </div>
            <ul className="list-disc ml-5 space-y-1">
              <li>“Summarize this document in 5 bullet points.”</li>
              <li>“What are the key risks and recommendations?”</li>
              <li>“Cite the exact section for this answer.”</li>
            </ul>
          </div>
        ) : (
          messages.map((m, i) => <MessageBubble key={i} msg={m} />)
        )}

        {isTyping && (
          <div className="flex justify-start">
            <div className="bg-slate-50 border border-slate-200 text-slate-500 rounded-2xl px-4 py-3 text-sm">
              Thinking…
            </div>
          </div>
        )}
      </div>

      <div className="border-t border-slate-200/80 p-3">
        <form onSubmit={onSubmit} className="flex gap-2">
          <input
            type="text"
            value={input}
            onChange={(e) => setInput(e.target.value)}
            placeholder={docId ? 'Ask about your document…' : 'Upload a PDF to begin…'}
            className="flex-1 rounded-xl border border-slate-300 bg-white px-4 py-3 text-sm shadow-sm focus:outline-none focus:ring-4 focus:ring-blue-500/20 focus:border-blue-500"
            disabled={isTyping}
          />
          <button
            type="submit"
            disabled={isTyping || !input.trim()}
            className={cx(
              'px-4 py-3 rounded-xl text-white transition inline-flex items-center justify-center shadow-sm',
              'bg-blue-600 hover:bg-blue-700 active:scale-[0.99] disabled:opacity-50 disabled:hover:bg-blue-600'
            )}
            title="Send"
          >
            <Send className="w-4 h-4" />
          </button>
        </form>

        <div className="mt-2 text-[11px] text-slate-500">
          Tip: Ask for a concise summary, action items, or page-wise citations.
        </div>
      </div>
    </section>
  );
}