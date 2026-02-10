/**
 * Centralized API client for all backend requests.
 * 
 * Ensures:
 * - No localhost URLs in production
 * - All /api/v1/* requests include Authorization header
 * - Proper error handling and messaging
 */

/**
 * Get the API base URL from environment or fallback.
 * 
 * @returns API base URL string
 * @throws Error if NEXT_PUBLIC_API_URL is missing in production
 */
export function getApiBaseUrl(): string {
  const envUrl = process.env.NEXT_PUBLIC_API_URL;
  
  // In production (non-localhost), require env var
  if (typeof window !== "undefined" && window.location.hostname !== "localhost") {
    if (!envUrl) {
      throw new Error("NEXT_PUBLIC_API_URL missing in production");
    }
    return envUrl;
  }
  
  // Development fallback to localhost
  return envUrl || "http://localhost:8000";
}

// Log base URL once on app load (dev only)
if (typeof window !== "undefined" && process.env.NODE_ENV !== "production") {
  console.log("[API] Base URL:", getApiBaseUrl());
}

interface ApiFetchOptions {
  token?: string | null;
  method?: string;
  body?: any | FormData;
  headers?: Record<string, string>;
}

/**
 * Unified fetch wrapper for API requests.
 * 
 * @param path - API path (e.g., "/api/v1/campaigns")
 * @param options - Fetch options including token, method, body, headers
 * @returns Promise resolving to parsed JSON response
 * @throws Error with descriptive message on failure
 */
export async function apiFetch<T = any>(
  path: string,
  options: ApiFetchOptions = {}
): Promise<T> {
  const { token, method = "GET", body, headers = {} } = options;
  
  // Require token for /api/v1/* endpoints
  if (path.startsWith("/api/v1/") && !token) {
    throw new Error("Missing auth token");
  }
  
  const baseUrl = getApiBaseUrl();
  const url = `${baseUrl}${path}`;
  
  // Build headers
  const requestHeaders: Record<string, string> = {
    ...headers,
  };
  
  // Add Authorization header if token provided
  if (token) {
    requestHeaders.Authorization = `Bearer ${token}`;
  }
  
  // Handle FormData vs JSON body
  const isFormData = body instanceof FormData;
  if (!isFormData) {
    requestHeaders["Content-Type"] = "application/json";
  }
  
  // Build request options
  const requestOptions: RequestInit = {
    method,
    headers: requestHeaders,
  };
  
  // Add body for non-GET requests
  if (body && method !== "GET") {
    requestOptions.body = isFormData ? body : JSON.stringify(body);
  }
  
  try {
    const response = await fetch(url, requestOptions);
    
    if (!response.ok) {
      // Try to parse error detail from response
      let errorDetail = `HTTP ${response.status}`;
      try {
        const errorData = await response.json();
        if (errorData.detail) {
          errorDetail = `HTTP ${response.status}: ${errorData.detail}`;
        } else if (errorData.message) {
          errorDetail = `HTTP ${response.status}: ${errorData.message}`;
        }
      } catch {
        // If JSON parse fails, use status text
        const statusText = response.statusText || "Unknown error";
        errorDetail = `HTTP ${response.status}: ${statusText}`;
      }
      throw new Error(errorDetail);
    }
    
    // Parse JSON response
    const data = await response.json();
    return data as T;
  } catch (error) {
    // Re-throw if already an Error with message
    if (error instanceof Error) {
      throw error;
    }
    // Network/CORS errors
    throw new Error("Network/CORS failure");
  }
}
