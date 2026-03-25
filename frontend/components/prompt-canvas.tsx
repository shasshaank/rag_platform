"use client";

import { useState } from "react";
import { Send, Wand2 } from "lucide-react";
import { Button } from "@/components/ui/button";
import { getPublicEnv } from "@/lib/env";
import type { Message } from "@/lib/types";

type Props = {
  selectedDocIds: string[];
  onAnswered: (userMessage: Message, assistantMessage: Message) => void;
};

export function PromptCanvas({ selectedDocIds, onAnswered }: Props) {
  const { chatApi } = getPublicEnv();
  const [prompt, setPrompt] = useState("");
  const [isTyping, setIsTyping] = useState(false);

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!prompt.trim() || isTyping) return;

    const userText = prompt.trim();
    setPrompt("");
    setIsTyping(true);

    const userMessage: Message = { role: "user", content: userText };

    try {
      if (selectedDocIds.length === 0) {
        const assistantMessage: Message = {
          role: "assistant",
          content: "Select at least one document first, then ask questions.",
        };
        onAnswered(userMessage, assistantMessage);
        return;
      }

      if (!chatApi) {
        const assistantMessage: Message = {
          role: "assistant",
          content: "Missing NEXT_PUBLIC_CHAT_API_URL in environment.",
        };
        onAnswered(userMessage, assistantMessage);
        return;
      }

      const res = await fetch(`${chatApi}/chat`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ question: userText, doc_ids: selectedDocIds }),
      });

      const data = await res.json().catch(() => ({} as any));

      const assistantMessage: Message = {
        role: "assistant",
        content: data.answer || data.error || "Unknown error occurred.",
        citations: Array.isArray(data.citations) ? data.citations : [],
      };

      onAnswered(userMessage, assistantMessage);
    } catch (err) {
      console.error(err);
      onAnswered(userMessage, {
        role: "assistant",
        content: "Network error. Is the Python server running?",
      });
    } finally {
      setIsTyping(false);
    }
  };

  return (
    <div className="rounded-2xl border border-border bg-card p-6 shadow-sm h-full flex flex-col">
      <div className="mb-4 flex items-center justify-between">
        <div>
          <h2 className="text-lg font-semibold text-foreground">Query Canvas</h2>
          <p className="text-sm text-muted-foreground">Ask questions about your indexed documents</p>
        </div>
        <div className="flex items-center gap-2 rounded-full bg-secondary/50 px-3 py-1.5 text-xs font-medium text-secondary-foreground">
          <span className={`h-2 w-2 rounded-full ${selectedDocIds.length > 0 ? 'bg-green-500' : 'bg-muted-foreground'}`} />
          <span>{selectedDocIds.length > 0 ? `${selectedDocIds.length} document(s)` : "No documents"}</span>
        </div>
      </div>

      <form onSubmit={handleSubmit} className="flex-1 flex flex-col">
        <textarea
          value={prompt}
          onChange={(e) => setPrompt(e.target.value)}
          placeholder={selectedDocIds.length > 0 ? "Ask a question about the selected documents..." : "Select at least one document..."}
          className="h-full min-h-[220px] lg:min-h-[280px] w-full resize-none rounded-xl border border-border bg-muted/30 p-4 text-foreground placeholder:text-muted-foreground focus:border-primary focus:ring-2 focus:ring-primary/20 focus:outline-none"
        />

        <div className="mt-4 flex flex-col sm:flex-row items-stretch sm:items-center justify-between gap-3">
          <div className="flex items-center gap-2 text-sm text-muted-foreground">
            <Wand2 className="h-4 w-4" />
            <span>AI-powered semantic search</span>
          </div>
          <Button
            type="submit"
            disabled={!prompt.trim() || isTyping}
            className="bg-primary text-primary-foreground hover:bg-primary/90 rounded-xl px-6 gap-2 disabled:opacity-50"
          >
            <Send className="h-4 w-4" />
            <span>{isTyping ? "Thinking..." : "Submit Query"}</span>
          </Button>
        </div>
      </form>
    </div>
  );
}