'use client';

import { useState } from 'react';
import { ChevronDown, ChevronRight } from 'lucide-react';
import type { Message } from '@/lib/types';

function cx(...classes: Array<string | false | undefined | null>) {
  return classes.filter(Boolean).join(' ');
}

export function MessageBubble({ msg }: { msg: Message }) {
  const [open, setOpen] = useState(false);

  const isUser = msg.role === 'user';
  const citationsCount = msg.role === 'assistant' && msg.citations ? msg.citations.length : 0;

  return (
    <div className={cx('flex', isUser ? 'justify-end' : 'justify-start')}>
      <div
        className={cx(
          'max-w-[90%] md:max-w-[85%] rounded-2xl px-4 py-3 text-sm leading-relaxed shadow-sm',
          isUser
            ? 'bg-gradient-to-r from-blue-600 to-indigo-600 text-white'
            : 'bg-white border border-slate-200 text-slate-900'
        )}
      >
        <div className="whitespace-pre-wrap">{msg.content}</div>

        {msg.role === 'assistant' && citationsCount > 0 && (
          <div className="mt-3 pt-3 border-t border-slate-200">
            <button
              type="button"
              className="text-xs font-medium text-slate-700 inline-flex items-center gap-1 hover:text-slate-900"
              onClick={() => setOpen((v) => !v)}
            >
              {open ? <ChevronDown className="w-4 h-4" /> : <ChevronRight className="w-4 h-4" />}
              Sources ({citationsCount})
            </button>

            {open && (
              <div className="mt-2 space-y-2">
                {(msg.citations || []).map((c) => (
                  <div key={c.idx} className="text-xs bg-slate-50 border border-slate-200 rounded-xl p-2.5">
                    <div className="text-slate-700">
                      <span className="font-semibold">[{c.idx}]</span>{' '}
                      <span className="font-medium">{c.filename ?? 'unknown'}</span>{' '}
                      {c.page != null ? `(page ${c.page})` : ''}{' '}
                      {c.chunk_id != null ? `• chunk ${c.chunk_id}` : ''}
                      {typeof c.score === 'number' ? (
                        <span className="text-slate-400"> • score {c.score.toFixed(3)}</span>
                      ) : null}
                    </div>

                    {c.text_preview && (
                      <div className="mt-1.5 text-slate-600 whitespace-pre-wrap">{c.text_preview}</div>
                    )}
                  </div>
                ))}
              </div>
            )}
          </div>
        )}
      </div>
    </div>
  );
}