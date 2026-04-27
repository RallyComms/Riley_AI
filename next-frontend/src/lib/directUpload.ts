/**
 * Direct-to-GCS upload client.
 *
 * 1. Calls the backend `/api/v1/uploads/direct/init` to get a signed PUT URL
 *    and a server-owned object path.
 * 2. PUTs the file bytes directly to GCS using that signed URL.
 * 3. Calls `/api/v1/uploads/direct/complete` so the backend creates the same
 *    parent-file metadata the legacy proxy upload would create.
 *
 * Bytes never go through Cloud Run, so request-body size limits do not apply.
 * The backend still enforces auth, tenant access, file size, and the object
 * path. Ingestion stays gated behind the Riley Memory toggle.
 */

import { apiFetch, ApiRequestError } from "@app/lib/api";

export type DirectUploadSurface = "assets" | "media";

export interface DirectUploadInitResponse {
  file_id: string;
  object_name: string;
  gcs_url: string;
  signed_upload_url: string;
  required_headers: Record<string, string>;
  expires_at: string;
  max_size_bytes: number;
}

export interface DirectUploadCompleteResponse {
  id: string;
  url: string;
  filename: string;
  type: string;
  preview_status?: string | null;
  preview_error?: string | null;
}

export interface DirectUploadOptions {
  token: string;
  tenantId: string;
  surface: DirectUploadSurface;
  file: File;
  tags?: string[];
  onProgress?: (uploadedBytes: number, totalBytes: number) => void;
  signal?: AbortSignal;
}

export class DirectUploadError extends Error {
  status?: number;
  cause?: unknown;

  constructor(message: string, options: { status?: number; cause?: unknown } = {}) {
    super(message);
    this.name = "DirectUploadError";
    this.status = options.status;
    this.cause = options.cause;
  }
}

function tooLargeMessage(maxMb: number): string {
  return `This file is too large. Current limit is ${Math.round(maxMb)} MB.`;
}

async function putToSignedUrl(
  signedUrl: string,
  file: File,
  headers: Record<string, string>,
  onProgress?: (uploadedBytes: number, totalBytes: number) => void,
  signal?: AbortSignal
): Promise<void> {
  // We use XHR rather than fetch so we can surface upload progress events.
  await new Promise<void>((resolve, reject) => {
    const xhr = new XMLHttpRequest();
    xhr.open("PUT", signedUrl, true);
    Object.entries(headers || {}).forEach(([key, value]) => {
      xhr.setRequestHeader(key, value);
    });

    if (xhr.upload && typeof onProgress === "function") {
      xhr.upload.onprogress = (event) => {
        if (event.lengthComputable) {
          onProgress(event.loaded, event.total);
        }
      };
    }

    xhr.onload = () => {
      if (xhr.status >= 200 && xhr.status < 300) {
        resolve();
      } else {
        reject(
          new DirectUploadError(
            `GCS rejected the direct upload (HTTP ${xhr.status}).`,
            { status: xhr.status, cause: xhr.responseText }
          )
        );
      }
    };
    xhr.onerror = () => {
      reject(new DirectUploadError("Network error while uploading to storage."));
    };
    xhr.onabort = () => {
      reject(new DirectUploadError("Upload was cancelled."));
    };

    if (signal) {
      const handleAbort = () => {
        try {
          xhr.abort();
        } catch {
          // ignore
        }
      };
      if (signal.aborted) {
        handleAbort();
      } else {
        signal.addEventListener("abort", handleAbort, { once: true });
      }
    }

    xhr.send(file);
  });
}

export async function uploadFileDirectToGcs(
  options: DirectUploadOptions
): Promise<DirectUploadCompleteResponse> {
  const { token, tenantId, surface, file, tags, onProgress, signal } = options;
  const contentType = file.type || "application/octet-stream";

  let init: DirectUploadInitResponse;
  try {
    init = await apiFetch<DirectUploadInitResponse>(
      "/api/v1/uploads/direct/init",
      {
        token,
        method: "POST",
        body: {
          tenant_id: tenantId,
          filename: file.name,
          content_type: contentType,
          size_bytes: file.size,
          surface,
          tags: tags ?? [],
        },
        signal,
      }
    );
  } catch (error) {
    if (error instanceof ApiRequestError && error.status === 413) {
      throw new DirectUploadError(error.message, { status: 413, cause: error });
    }
    throw error;
  }

  if (file.size > init.max_size_bytes) {
    throw new DirectUploadError(tooLargeMessage(init.max_size_bytes / (1024 * 1024)), {
      status: 413,
    });
  }

  const headers: Record<string, string> = { ...(init.required_headers || {}) };
  if (!headers["Content-Type"]) {
    headers["Content-Type"] = contentType;
  }

  await putToSignedUrl(init.signed_upload_url, file, headers, onProgress, signal);

  const result = await apiFetch<DirectUploadCompleteResponse>(
    "/api/v1/uploads/direct/complete",
    {
      token,
      method: "POST",
      body: {
        tenant_id: tenantId,
        file_id: init.file_id,
        object_name: init.object_name,
        filename: file.name,
        content_type: contentType,
        size_bytes: file.size,
        surface,
        tags: tags ?? [],
      },
      signal,
    }
  );
  return result;
}
