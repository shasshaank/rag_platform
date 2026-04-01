"use client";

import { useState, useCallback, useEffect } from "react";
import { Navbar } from "@/components/navbar";
import { PromptCanvas } from "@/components/prompt-canvas";
import { UploadPanel } from "@/components/upload-panel";
import { AnswerPreview } from "@/components/answer-preview";
import { Footer } from "@/components/footer";
import { ConversationSidebar, type Conversation } from "@/components/conversation-sidebar";
import { createClient } from "@/lib/supabase/client";
import type { Message } from "@/lib/types";

export default function DashboardPage() {
  const supabase = createClient();

  const [documents, setDocuments] = useState<{ id: string; name: string }[]>([]);
  const [selectedDocIds, setSelectedDocIds] = useState<string[]>([]);
  const [messages, setMessages] = useState<Message[]>([]);
  const [processingDocIds, setProcessingDocIds] = useState<Set<string>>(new Set());

  // Conversation state
  const [conversations, setConversations] = useState<Conversation[]>([]);
  const [activeConversationId, setActiveConversationId] = useState<string | null>(null);
  const [userId, setUserId] = useState<string | null>(null);

  // Get current user and load conversations
  useEffect(() => {
    const init = async () => {
      const { data: { user } } = await supabase.auth.getUser();
      if (!user) return;
      setUserId(user.id);
      await loadConversations(user.id);
    };
    init();
  }, []);

  const loadConversations = async (uid: string) => {
    const { data, error } = await supabase
      .from("conversations")
      .select("*")
      .eq("user_id", uid)
      .order("updated_at", { ascending: false });

    if (!error && data) {
      setConversations(data);
    }
  };

  const loadMessages = async (conversationId: string) => {
    const { data, error } = await supabase
      .from("messages")
      .select("*")
      .eq("conversation_id", conversationId)
      .order("created_at", { ascending: true });

    if (!error && data) {
      const msgs: Message[] = data.map((m: any) => ({
        role: m.role,
        content: m.content,
        ...(m.role === "assistant" && {
          citations: m.citations || [],
          source_type: m.source_type || "document",
        }),
      }));
      setMessages(msgs);
    }
  };

  const createConversation = async (firstQuestion: string): Promise<string | null> => {
    if (!userId) return null;

    // Auto-title from first question (truncate to 60 chars)
    const title = firstQuestion.length > 60
      ? firstQuestion.substring(0, 57) + "..."
      : firstQuestion;

    const { data, error } = await supabase
      .from("conversations")
      .insert({
        user_id: userId,
        title,
        doc_ids: selectedDocIds,
      })
      .select()
      .single();

    if (!error && data) {
      setConversations((prev) => [data, ...prev]);
      setActiveConversationId(data.id);
      return data.id;
    }
    return null;
  };

  const saveMessages = async (conversationId: string, userMsg: Message, assistantMsg: Message) => {
    // Save user message
    await supabase.from("messages").insert({
      conversation_id: conversationId,
      role: "user",
      content: userMsg.content,
    });

    // Save assistant message
    const assistantData: any = {
      conversation_id: conversationId,
      role: "assistant",
      content: assistantMsg.content,
    };
    if ("citations" in assistantMsg) {
      assistantData.citations = assistantMsg.citations || [];
    }
    if ("source_type" in assistantMsg) {
      assistantData.source_type = assistantMsg.source_type || "document";
    }
    await supabase.from("messages").insert(assistantData);

    // Update conversation timestamp
    await supabase
      .from("conversations")
      .update({ updated_at: new Date().toISOString() })
      .eq("id", conversationId);
  };

  const handleAnswered = async (userMsg: Message, assistantMsg: Message) => {
    setMessages((prev) => [...prev, userMsg, assistantMsg]);

    let convId = activeConversationId;
    if (!convId) {
      // Create a new conversation with auto-title from first question
      convId = await createConversation(userMsg.content);
    }
    if (convId) {
      await saveMessages(convId, userMsg, assistantMsg);
    }
  };

  const handleSelectConversation = async (id: string) => {
    setActiveConversationId(id);
    await loadMessages(id);

    // Load the doc_ids from the conversation
    const conv = conversations.find((c) => c.id === id);
    if (conv?.doc_ids) {
      setSelectedDocIds(conv.doc_ids);
    }
  };

  const handleNewConversation = () => {
    setActiveConversationId(null);
    setMessages([]);
  };

  const handleDeleteConversation = async (id: string) => {
    // Delete messages first (cascade should handle this, but be explicit)
    await supabase.from("messages").delete().eq("conversation_id", id);
    await supabase.from("conversations").delete().eq("id", id);
    setConversations((prev) => prev.filter((c) => c.id !== id));

    if (activeConversationId === id) {
      setActiveConversationId(null);
      setMessages([]);
    }
  };

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

  const hasProcessingDocs = selectedDocIds.some((id) => processingDocIds.has(id));

  return (
    <div className="min-h-screen flex flex-col bg-background">
      <Navbar />

      <main className="flex-1">
        <div className="mx-auto max-w-[1400px] px-4 py-6 sm:px-6 lg:px-8">
          <div className="flex gap-6">
            {/* Conversation Sidebar */}
            <div className="hidden lg:block w-64 shrink-0">
              <div className="sticky top-20 rounded-2xl border border-border bg-card shadow-sm overflow-hidden" style={{ maxHeight: "calc(100vh - 120px)" }}>
                <ConversationSidebar
                  conversations={conversations}
                  activeConversationId={activeConversationId}
                  onSelect={handleSelectConversation}
                  onNew={handleNewConversation}
                  onDelete={handleDeleteConversation}
                />
              </div>
            </div>

            {/* Main Content */}
            <div className="flex-1 min-w-0">
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
                    onAnswered={handleAnswered}
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
                      setActiveConversationId(null);
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
          </div>
        </div>
      </main>

      <Footer />
    </div>
  );
}