'use client';

import type { Message } from '@/lib/types';
import { FileText, Server, MessageCircleMore } from 'lucide-react';

export function RightPanel({
  docId,
  messages,
  gatewayOk,
  chatOk,
}: {
  docId: string | null;
  messages: Message[];
  gatewayOk: boolean;
  chatOk: boolean;
}) {
  let latestAssistant = null as null | Extract<Message, { role: 'assistant' }>;
  for (let i = messages.length - 1; i >= 0; i--) {
    const m = messages[i];
    if (m.role === 'assistant' && m.citations && m.citations.length) {
      latestAssistant = m;
      break;
    }
  }

  return (
    <aside className="rounded-2xl border border-slate-200/80 bg-white shadow-sm flex flex-col min-h-0">
      <div className="px-4 py-3 border-b border-slate-200/80">
        <div className="text-sm font-semibold flex items-center gap-2">
          <FileText className="w-4 h-4 text-blue-600" />
          Document Insights
        </div>
        <div className="text-xs text-slate-500">
          {docId ? 'Showing sources from your latest answer.' : 'Upload a PDF to see source chunks.'}
        </div>
      </div>

      <div className="flex-1 min-h-0 overflow-y-auto px-4 py-4">
        {!docId ? (
          <div className="text-sm text-slate-500 rounded-xl border border-dashed border-slate-300 bg-slate-50 p-3">
            No document uploaded yet.
          </div>
        ) : (
          <>
            <div className="text-xs text-slate-500">
              doc_id: <span className="font-mono text-slate-700">{docId}</span>
            </div>

            <div className="mt-4">
              <div className="text-sm font-medium flex items-center gap-2">
                <MessageCircleMore className="w-4 h-4 text-violet-600" />
                Latest sources
              </div>
              <div className="text-xs text-slate-500">
                Chunks used for the most recent assistant response.
              </div>

              <div className="mt-3 space-y-2">
                {latestAssistant?.citations?.length ? (
                  latestAssistant.citations.map((c) => (
                    <div key={c.idx} className="border border-slate-200 rounded-xl p-3 bg-slate-50">
                      <div className="text-xs text-slate-700">
                        <span className="font-semibold">[{c.idx}]</span>{' '}
                        {c.filename ?? 'unknown'} {c.page != null ? `(page ${c.page})` : ''}
                      </div>
                      {c.text_preview && (
                        <div className="mt-1 text-xs text-slate-600 whitespace-pre-wrap">
                          {c.text_preview}
                        </div>
                      )}
                    </div>
                  ))
                ) : (
                  <div className="text-sm text-slate-500">Ask a question to view sources here.</div>
                )}
              </div>
            </div>
          </>
        )}
      </div>

      <div className="border-t border-slate-200/80 p-3 text-xs text-slate-500">
        <div className="inline-flex items-center gap-2 mr-4">
          <Server className="w-3.5 h-3.5" />
          Gateway:
          <span
            className={`px-2 py-0.5 rounded-full ${
              gatewayOk ? 'bg-emerald-50 text-emerald-700' : 'bg-rose-50 text-rose-700'
            }`}
          >
            {gatewayOk ? 'OK' : 'Missing env'}
          </span>
        </div>

        <div className="inline-flex items-center gap-2">
          Chat:
          <span
            className={`px-2 py-0.5 rounded-full ${
              chatOk ? 'bg-emerald-50 text-emerald-700' : 'bg-rose-50 text-rose-700'
            }`}
          >
            {chatOk ? 'OK' : 'Missing env'}
          </span>
        </div>
      </div>
    </aside>
  );
}