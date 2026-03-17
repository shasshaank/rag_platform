"use client";

import { useRef, useState } from "react";
import { Upload, FileText, CheckCircle2, AlertCircle } from "lucide-react";
import { Button } from "@/components/ui/button";
import { getPublicEnv } from "@/lib/env";

type Props = {
  docId: string | null;
  onDocUploaded: (docId: string, filename: string) => void;
};

export function UploadPanel({ docId, onDocUploaded }: Props) {
  const { gateway } = getPublicEnv();
  const inputRef = useRef<HTMLInputElement>(null);

  const [isUploading, setIsUploading] = useState(false);
  const [recentFiles, setRecentFiles] = useState<string[]>([]);

  const uploadFile = async (file: File) => {
    if (!gateway) {
      alert("Missing NEXT_PUBLIC_GATEWAY_URL in environment.");
      return;
    }

    const formData = new FormData();
    formData.append("file", file);

    setIsUploading(true);
    try {
      const res = await fetch(`${gateway}/upload`, {
        method: "POST",
        body: formData,
      });

      const data = await res.json().catch(() => null);

      if (res.ok && data?.job_id) {
        onDocUploaded(data.job_id, file.name);
        setRecentFiles((prev) => [file.name, ...prev.filter((f) => f !== file.name)].slice(0, 5));
      } else {
        alert(`Failed to upload file.${data?.error ? ` ${data.error}` : ""}`);
      }
    } catch (err) {
      console.error(err);
      alert("Upload failed. Is the gateway server running?");
    } finally {
      setIsUploading(false);
    }
  };

  const onInputChange = async (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0];
    if (!file) return;
    await uploadFile(file);
    e.target.value = "";
  };

  const onDrop = async (e: React.DragEvent<HTMLDivElement>) => {
    e.preventDefault();
    const file = e.dataTransfer.files?.[0];
    if (!file) return;
    await uploadFile(file);
  };

  return (
    <div className="rounded-2xl border border-border bg-card p-5 shadow-sm">
      <div className="mb-4">
        <h3 className="text-base font-semibold text-foreground">Upload Documents</h3>
        <p className="text-sm text-muted-foreground">Add files to your knowledge index</p>
      </div>

      <div
        onDragOver={(e) => e.preventDefault()}
        onDrop={onDrop}
        className="rounded-xl border-2 border-dashed border-secondary bg-secondary/30 p-6 text-center"
      >
        <div className="flex flex-col items-center gap-2">
          <div className="rounded-full bg-accent/30 p-3">
            <Upload className="h-6 w-6 text-accent-foreground" />
          </div>
          <p className="text-sm font-medium text-foreground">
            Drop file here or{" "}
            <button
              className="text-primary underline"
              onClick={() => inputRef.current?.click()}
              type="button"
            >
              browse
            </button>
          </p>
          <p className="text-xs text-muted-foreground">PDF up to 10MB</p>
          <input
            ref={inputRef}
            type="file"
            accept=".pdf"
            className="hidden"
            onChange={onInputChange}
          />
        </div>
      </div>

      <div className="mt-4 space-y-2">
        <p className="text-xs font-medium uppercase tracking-wider text-muted-foreground">Indexed Files</p>

        {docId ? (
          <div className="flex items-center gap-2 rounded-lg bg-muted/50 px-3 py-2.5">
            <CheckCircle2 className="h-4 w-4 text-green-500" />
            <div className="min-w-0">
              <p className="truncate text-sm font-medium text-foreground">Current document loaded</p>
              <p className="text-xs text-muted-foreground font-mono">{docId}</p>
            </div>
          </div>
        ) : (
          <div className="flex items-center gap-2 rounded-lg bg-muted/40 px-3 py-2.5 text-sm text-muted-foreground">
            <AlertCircle className="h-4 w-4" />
            No document uploaded yet
          </div>
        )}

        {recentFiles.map((name) => (
          <div key={name} className="flex items-center gap-2 rounded-lg bg-muted/30 px-3 py-2">
            <FileText className="h-4 w-4 text-muted-foreground" />
            <p className="truncate text-sm text-foreground">{name}</p>
          </div>
        ))}
      </div>

      <Button
        disabled={isUploading}
        className="mt-4 w-full bg-accent text-accent-foreground hover:bg-accent/80 rounded-xl"
      >
        {isUploading ? "Uploading..." : "Index Documents"}
      </Button>
    </div>
  );
}