"use client";

import { useRef, useState } from "react";
import { Upload, FileText, CheckCircle2, AlertCircle } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Checkbox } from "@/components/ui/checkbox";
import { getPublicEnv } from "@/lib/env";

type Props = {
  documents: { id: string; name: string }[];
  selectedDocIds: string[];
  onDocUploaded: (docId: string, filename: string) => void;
  onToggleSelection: (docId: string) => void;
};

export function UploadPanel({ documents, selectedDocIds, onDocUploaded, onToggleSelection }: Props) {
  const { gateway } = getPublicEnv();
  const inputRef = useRef<HTMLInputElement>(null);

  const [isUploading, setIsUploading] = useState(false);

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

        {documents.length === 0 ? (
          <div className="flex items-center gap-2 rounded-lg bg-muted/40 px-3 py-2.5 text-sm text-muted-foreground">
            <AlertCircle className="h-4 w-4" />
            No documents uploaded yet
          </div>
        ) : (
          <div className="space-y-2 max-h-48 overflow-y-auto pr-1">
            {documents.map((doc) => {
              const checked = selectedDocIds.includes(doc.id);
              return (
                <div key={doc.id} className="flex items-start gap-3 rounded-lg bg-muted/40 px-3 py-2.5">
                  <Checkbox 
                    id={`doc-${doc.id}`}
                    checked={checked} 
                    onCheckedChange={() => onToggleSelection(doc.id)}
                    className="mt-0.5 w-4 h-4"
                  />
                  <div className="min-w-0 flex-1">
                    <label htmlFor={`doc-${doc.id}`} className="truncate text-sm font-medium text-foreground leading-none mb-1 cursor-pointer select-none line-clamp-1 block">
                      {doc.name}
                    </label>
                    <p className="text-xs text-muted-foreground font-mono truncate">{doc.id}</p>
                  </div>
                </div>
              );
            })}
          </div>
        )}
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