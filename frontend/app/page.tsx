"use client";

import { useState, useCallback } from "react";
import { Navbar } from "@/components/navbar";
import { PromptCanvas } from "@/components/prompt-canvas";
import { UploadPanel } from "@/components/upload-panel";
import { AnswerPreview } from "@/components/answer-preview";
import { Footer } from "@/components/footer";
import type { Message } from "@/lib/types";

export default function DashboardPage() {
  const [documents, setDocuments] = useState<{ id: string; name: string }[]>([]);
  const [selectedDocIds, setSelectedDocIds] = useState<string[]>([]);
  const [messages, setMessages] = useState<Message[]>([]);
  const [processingDocIds, setProcessingDocIds] = useState<Set<string>>(new Set());

  const markProcessing = useCallback((docId: string) => {
    setProcessingDocIds((prev) => new Set(prev).add(docId));
  }, []);

  const markReady = useCallback((docId: string) => {
    setProcessingDocIds((prev) => {
      const next = new Set(prev);
      next.delete(docId);
      return next;
    });
  }, []);

  // Check if any selected documents are still processing
  const hasProcessingDocs = selectedDocIds.some((id) => processingDocIds.has(id));

  return (
    <div className="min-h-screen flex flex-col bg-background">
      <Navbar />

      <main className="flex-1">
        <div className="mx-auto max-w-7xl px-4 py-6 sm:px-6 lg:px-8">
          <div className="mb-6">
            <h1 className="text-2xl font-bold tracking-tight text-foreground sm:text-3xl">
              Dashboard
            </h1>
            <p className="mt-1 text-sm text-muted-foreground">
              Query your documents and manage your knowledge index
            </p>
            {selectedDocIds.length > 0 && (
              <p className="mt-2 text-xs text-muted-foreground">
                Active docs: <span className="font-mono">{selectedDocIds.length} selected</span>
                {hasProcessingDocs && (
                  <span className="ml-2 text-amber-500 font-medium">
                    ⏳ Some documents are still being indexed...
                  </span>
                )}
              </p>
            )}
          </div>

          <div className="grid gap-6 lg:grid-cols-12">
            <div className="lg:col-span-8">
              <PromptCanvas
                selectedDocIds={selectedDocIds}
                messages={messages}
                onAnswered={(u, a) => setMessages((prev) => [...prev, u, a])}
                hasProcessingDocs={hasProcessingDocs}
              />
            </div>

            <div className="lg:col-span-4">
              <UploadPanel
                documents={documents}
                selectedDocIds={selectedDocIds}
                processingDocIds={processingDocIds}
                onDocUploaded={(id, name) => {
                  setDocuments((prev) => [{ id, name }, ...prev]);
                  if (!selectedDocIds.includes(id)) {
                    setSelectedDocIds((prev) => [...prev, id]);
                  }
                  setMessages([]);
                }}
                onToggleSelection={(id) => {
                  setSelectedDocIds((prev) =>
                    prev.includes(id) ? prev.filter((d) => d !== id) : [...prev, id]
                  );
                }}
                onDocDeleted={(id) => {
                  setDocuments((prev) => prev.filter((d) => d.id !== id));
                  setSelectedDocIds((prev) => prev.filter((d) => d !== id));
                }}
                markProcessing={markProcessing}
                markReady={markReady}
              />
            </div>
          </div>

          <AnswerPreview messages={messages} />
        </div>
      </main>

      <Footer />
    </div>
  );
}