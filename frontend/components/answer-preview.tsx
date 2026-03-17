import { FileText, ExternalLink, ThumbsUp, ThumbsDown, Copy } from "lucide-react";
import { Button } from "@/components/ui/button";

interface AnswerCardProps {
  query: string;
  answer: string;
  sources: { name: string; page?: number }[];
  timestamp: string;
}

function AnswerCard({ query, answer, sources, timestamp }: AnswerCardProps) {
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

      <div className="mb-4">
        <p className="text-xs font-medium uppercase tracking-wider text-muted-foreground mb-2">
          Sources
        </p>
        <div className="flex flex-wrap gap-2">
          {sources.map((source, index) => (
            <span
              key={index}
              className="inline-flex items-center gap-1.5 rounded-lg bg-secondary/50 px-2.5 py-1.5 text-xs font-medium text-secondary-foreground"
            >
              <FileText className="h-3 w-3" aria-hidden="true" />
              {source.name}
              {source.page && <span className="text-muted-foreground">p.{source.page}</span>}
            </span>
          ))}
        </div>
      </div>

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

const sampleAnswers: AnswerCardProps[] = [
  {
    query: "What are the Q4 revenue projections?",
    answer:
      "Based on the Q4 report, projected revenue is $4.2M with a 15% growth rate compared to Q3. The primary growth drivers are the enterprise segment and new product launches planned for November.",
    sources: [
      { name: "q4-report.docx", page: 12 },
      { name: "financial-summary.pdf", page: 3 },
    ],
    timestamp: "5 min ago",
  },
  {
    query: "What is the company's remote work policy?",
    answer:
      "The company follows a hybrid work model where employees can work remotely up to 3 days per week. All remote work must be coordinated with team leads, and core collaboration hours are 10am-3pm in the employee's local timezone.",
    sources: [{ name: "company-handbook.pdf", page: 24 }],
    timestamp: "12 min ago",
  },
];

export function AnswerPreview() {
  return (
    <section className="mt-8">
      <div className="mb-5 flex items-center justify-between">
        <div>
          <h2 className="text-lg font-semibold text-foreground">Recent Answers</h2>
          <p className="text-sm text-muted-foreground">
            Previously generated responses from your queries
          </p>
        </div>
        <Button
          variant="ghost"
          size="sm"
          className="text-primary hover:text-primary/80 hover:bg-primary/10"
        >
          View History
        </Button>
      </div>

      <div className="grid gap-4 md:grid-cols-2">
        {sampleAnswers.map((answer, index) => (
          <AnswerCard key={index} {...answer} />
        ))}
      </div>
    </section>
  );
}
