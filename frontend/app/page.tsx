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
  const [docId, setDocId] = useState<string | null>(null);
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
            {docId && (
              <p className="mt-2 text-xs text-muted-foreground">
                Active doc_id: <span className="font-mono">{docId}</span>
              </p>
            )}
          </div>

          <div className="grid gap-6 lg:grid-cols-12">
            <div className="lg:col-span-5 xl:col-span-6">
              <PromptCanvas
                docId={docId}
                onAnswered={(u, a) => setMessages((prev) => [...prev, u, a])}
              />
            </div>

            <div className="lg:col-span-4 xl:col-span-3">
              <UploadPanel
                docId={docId}
                onDocUploaded={(id) => {
                  setDocId(id);
                  setMessages([]);
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