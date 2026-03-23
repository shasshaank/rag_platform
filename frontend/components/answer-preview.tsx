import { FileText, ExternalLink, ThumbsUp, ThumbsDown, Copy } from "lucide-react";
import { Button } from "@/components/ui/button";
import type { Message } from "@/lib/types";

interface AnswerCardProps {
  query: string;
  answer: string;
  sources?: any[];
  timestamp?: string;
}

function AnswerCard({ query, answer, sources = [], timestamp }: AnswerCardProps) {
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
              <span
                key={index}
                className="inline-flex items-center gap-1.5 rounded-lg bg-secondary/50 px-2.5 py-1.5 text-xs font-medium text-secondary-foreground"
              >
                <FileText className="h-3 w-3" aria-hidden="true" />
                {typeof source === 'string' ? source : (source.name || 'document')}
                {source.page && <span className="text-muted-foreground">p.{source.page}</span>}
              </span>
            ))}
          </div>
        </div>
      )}

      <div className="flex items-center justify-between pt-3 border-t border-border">
        <div className="flex items-center gap-1">
          <Button
            variant="ghost"
            size="sm"
            className="h-8 px-2 text-muted-foreground hover:text-foreground"
            aria-label="Mark as helpful"
          >
            <ThumbsUp className="h-3.5 w-3.5" />
          </Button>
          <Button
            variant="ghost"
            size="sm"
            className="h-8 px-2 text-muted-foreground hover:text-foreground"
            aria-label="Mark as not helpful"
          >
            <ThumbsDown className="h-3.5 w-3.5" />
          </Button>
        </div>
        <div className="flex items-center gap-1">
          <Button
            variant="ghost"
            size="sm"
            className="h-8 px-2 text-muted-foreground hover:text-foreground gap-1.5"
          >
            <Copy className="h-3.5 w-3.5" aria-hidden="true" />
            <span className="text-xs">Copy</span>
          </Button>
          <Button
            variant="ghost"
            size="sm"
            className="h-8 px-2 text-muted-foreground hover:text-foreground gap-1.5"
          >
            <ExternalLink className="h-3.5 w-3.5" aria-hidden="true" />
            <span className="text-xs">Expand</span>
          </Button>
        </div>
      </div>
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
