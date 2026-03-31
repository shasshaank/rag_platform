"use client";

import { useState } from "react";
import { FileText, ExternalLink, ThumbsUp, ThumbsDown, Copy } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Dialog, DialogContent, DialogHeader, DialogTitle } from "@/components/ui/dialog";
import type { Message } from "@/lib/types";

interface AnswerCardProps {
  query: string;
  answer: string;
  sources?: any[];
  timestamp?: string;
}

function AnswerCard({ query, answer, sources = [], timestamp }: AnswerCardProps) {
  const [selectedSource, setSelectedSource] = useState<any | null>(null);
  const [isLiked, setIsLiked] = useState<boolean | null>(null);
  const [isCopied, setIsCopied] = useState(false);
  const [isExpanded, setIsExpanded] = useState(false);

  return (
    <article className="rounded-2xl border border-border bg-card p-5 shadow-sm">
      <div className="mb-3 flex items-start justify-between gap-4">
        <div>
          <p className="text-xs font-medium uppercase tracking-wider text-muted-foreground mb-1">
            Query
          </p>
          <h4 className="text-sm font-semibold text-foreground leading-snug">{query}</h4>
        </div>
        <span className="shrink-0 text-xs text-muted-foreground">{timestamp}</span>
      </div>

      <div className="mb-4">
        <p className="text-xs font-medium uppercase tracking-wider text-muted-foreground mb-1.5">
          Answer
        </p>
        <p className="text-sm text-foreground leading-relaxed">{answer}</p>
      </div>

      {sources && sources.length > 0 && (
        <div className="mb-4">
          <p className="text-xs font-medium uppercase tracking-wider text-muted-foreground mb-2">
            Sources
          </p>
          <div className="flex flex-wrap gap-2">
            {sources.map((source: any, index: number) => (
              <button
                key={index}
                onClick={() => setSelectedSource(source)}
                className="inline-flex items-center gap-1.5 rounded-lg bg-secondary/50 px-2.5 py-1.5 text-xs font-medium text-secondary-foreground hover:bg-secondary transition-colors"
                title="View document citation"
              >
                <FileText className="h-3 w-3" aria-hidden="true" />
                {typeof source === 'string' ? source : (source.filename || source.name || 'document')}
                {source.page && <span className="text-muted-foreground">p.{source.page}</span>}
              </button>
            ))}
          </div>
        </div>
      )}

      <div className="flex items-center justify-between pt-3 border-t border-border">
        <div className="flex items-center gap-1">
          <Button
            variant="ghost"
            size="sm"
            className={`h-8 px-2 hover:text-foreground ${isLiked === true ? 'text-primary' : 'text-muted-foreground'}`}
            aria-label="Mark as helpful"
            onClick={() => setIsLiked(true)}
          >
            <ThumbsUp className="h-3.5 w-3.5" />
          </Button>
          <Button
            variant="ghost"
            size="sm"
            className={`h-8 px-2 hover:text-foreground ${isLiked === false ? 'text-destructive' : 'text-muted-foreground'}`}
            aria-label="Mark as not helpful"
            onClick={() => setIsLiked(false)}
          >
            <ThumbsDown className="h-3.5 w-3.5" />
          </Button>
        </div>
        <div className="flex items-center gap-1">
          <Button
            variant="ghost"
            size="sm"
            className="h-8 px-2 text-muted-foreground hover:text-foreground gap-1.5"
            onClick={() => {
              navigator.clipboard.writeText(answer);
              setIsCopied(true);
              setTimeout(() => setIsCopied(false), 2000);
            }}
          >
            <Copy className="h-3.5 w-3.5" aria-hidden="true" />
            <span className="text-xs">{isCopied ? "Copied!" : "Copy"}</span>
          </Button>
          <Button
            variant="ghost"
            size="sm"
            className="h-8 px-2 text-muted-foreground hover:text-foreground gap-1.5"
            onClick={() => setIsExpanded(true)}
          >
            <ExternalLink className="h-3.5 w-3.5" aria-hidden="true" />
            <span className="text-xs">Expand</span>
          </Button>
        </div>
      </div>

      <Dialog open={!!selectedSource} onOpenChange={(open) => !open && setSelectedSource(null)}>
        <DialogContent className="max-w-xl max-h-[80vh] overflow-y-auto w-[90vw] sm:w-full">
          <DialogHeader>
            <DialogTitle className="flex items-center gap-2">
              <FileText className="h-5 w-5" />
              {selectedSource?.filename || selectedSource?.name || 'Document Citation'}
              {selectedSource?.page && <span className="text-muted-foreground text-sm font-normal"> - Page {selectedSource.page}</span>}
            </DialogTitle>
          </DialogHeader>
          <div className="mt-4 rounded-md bg-muted/50 p-4 text-sm leading-relaxed text-foreground whitespace-pre-wrap border font-serif">
            {selectedSource?.text || selectedSource?.text_preview || "No text available."}
          </div>
        </DialogContent>
      </Dialog>
      
      <Dialog open={isExpanded} onOpenChange={setIsExpanded}>
        <DialogContent className="max-w-2xl max-h-[80vh] overflow-y-auto w-[90vw] sm:w-full">
          <DialogHeader>
            <DialogTitle>Answer Detail</DialogTitle>
          </DialogHeader>
          <div className="mt-4">
            <h4 className="text-sm font-semibold mb-2">Query</h4>
            <p className="text-sm bg-muted/30 border border-border p-3 rounded-md mb-4">{query}</p>
            <h4 className="text-sm font-semibold mb-2">Answer</h4>
            <div className="text-sm leading-relaxed text-foreground whitespace-pre-wrap bg-muted/10 p-4 rounded-md border border-border">{answer}</div>
          </div>
        </DialogContent>
      </Dialog>
    </article>
  );
}

export function AnswerPreview({ messages }: { messages: Message[] }) {
  const pairs = [];
  for (let i = 0; i < messages.length - 1; i++) {
    if (messages[i].role === "user" && messages[i + 1].role === "assistant") {
      pairs.push({
        query: messages[i].content,
        answer: messages[i + 1].content,
        sources: (messages[i + 1] as any).citations || [],
      });
    }
  }

  if (pairs.length === 0) return null;

  // Reverse so newest is first
  pairs.reverse();

  return (
    <section className="mt-8">
      <div className="mb-5 flex items-center justify-between">
        <div>
          <h2 className="text-lg font-semibold text-foreground">Recent Answers</h2>
          <p className="text-sm text-muted-foreground">
            Previously generated responses from your queries
          </p>
        </div>
      </div>

      <div className="grid gap-4 md:grid-cols-2">
        {pairs.map((pair, index) => (
          <AnswerCard 
            key={index} 
            query={pair.query} 
            answer={pair.answer} 
            sources={pair.sources} 
            timestamp="Just now" 
          />
        ))}
      </div>
    </section>
  );
}
