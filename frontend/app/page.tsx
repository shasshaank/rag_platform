"use client";

import { useState } from "react";
import { Navbar } from "@/components/navbar";
import { PromptCanvas } from "@/components/prompt-canvas";
import { UploadPanel } from "@/components/upload-panel";
import { ActivityFeed, IndexUsage, RecentQueries } from "@/components/sidebar-cards";
import { AnswerPreview } from "@/components/answer-preview";
import { Footer } from "@/components/footer";
import type { Message } from "@/lib/types";

export default function DashboardPage() {
  const [documents, setDocuments] = useState<{ id: string; name: string }[]>([]);
  const [selectedDocIds, setSelectedDocIds] = useState<string[]>([]);
  const [messages, setMessages] = useState<Message[]>([]);

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
              </p>
            )}
          </div>

          <div className="grid gap-6 lg:grid-cols-12">
            <div className="lg:col-span-5 xl:col-span-6">
              <PromptCanvas
                selectedDocIds={selectedDocIds}
                onAnswered={(u, a) => setMessages((prev) => [...prev, u, a])}
              />
            </div>

            <div className="lg:col-span-4 xl:col-span-3">
              <UploadPanel
                documents={documents}
                selectedDocIds={selectedDocIds}
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
              />
            </div>

            <aside className="lg:col-span-3 space-y-4">
              <ActivityFeed />
              <IndexUsage />
              <RecentQueries />
            </aside>
          </div>

          <AnswerPreview messages={messages} />
        </div>
      </main>

      <Footer />
    </div>
  );
}