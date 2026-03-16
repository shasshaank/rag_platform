'use client';

import type { Message } from '@/lib/types';
import { FileText } from 'lucide-react';

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
    <div className="bg-white border rounded-2xl flex flex-col min-h-0 shadow-sm">
      <div className="px-4 py-3 border-b">
        <div className="text-sm font-semibold flex items-center gap-2">
          <FileText className="w-4 h-4 text-blue-600" />
          Document
        </div>
        <div className="text-xs text-slate-500">
          {docId ? 'Showing sources from the latest answer.' : 'Upload a PDF to see sources.'}
        </div>
      </div>

      <div className="flex-1 min-h-0 overflow-y-auto px-4 py-4">
        {!docId ? (
          <div className="text-sm text-slate-500">No document uploaded yet.</div>
        ) : (
          <>
            <div className="text-xs text-slate-500">
              doc_id: <span className="font-mono">{docId}</span>
            </div>

            <div className="mt-4">
              <div className="text-sm font-medium">Latest sources</div>
              <div className="text-xs text-slate-500">
                These are the chunks used for the most recent assistant answer.
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
                  <div className="text-sm text-slate-500">Ask a question to see sources here.</div>
                )}
              </div>
            </div>
          </>
        )}
      </div>

      <div className="border-t p-3 text-xs text-slate-500">
        Backend:{' '}
        <span className={gatewayOk ? 'text-emerald-600' : 'text-rose-600'}>
          {gatewayOk ? 'Gateway OK' : 'Gateway missing env'}
        </span>
        {' • '}
        <span className={chatOk ? 'text-emerald-600' : 'text-rose-600'}>
          {chatOk ? 'Chat OK' : 'Chat missing env'}
        </span>
      </div>
    </div>
  );
}