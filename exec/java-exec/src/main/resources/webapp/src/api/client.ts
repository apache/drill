/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import axios, { AxiosError, AxiosInstance } from 'axios';

const CSRF_TOKEN_ENDPOINT = '/api/v1/csrf-token';
const CSRF_HEADER = 'X-CSRF-Token';
const MUTATING_METHODS = new Set(['post', 'put', 'delete', 'patch']);

const apiClient: AxiosInstance = axios.create({
  baseURL: '',
  withCredentials: true,
  headers: {
    'Content-Type': 'application/json',
  },
});

// Cached promise so concurrent boot-time requests share a single fetch.
let csrfTokenPromise: Promise<string | null> | null = null;

async function fetchCsrfToken(): Promise<string | null> {
  try {
    const response = await axios.get<{ token?: string }>(CSRF_TOKEN_ENDPOINT, {
      withCredentials: true,
    });
    const token = response.data?.token ?? null;
    if (token) {
      // Mirror into the meta tag so non-axios readers (Pyodide, observability,
      // anything else doing manual fetch) can pull from a sync DOM source.
      const meta = document.querySelector('meta[name="csrf-token"]');
      if (meta) {
        meta.setAttribute('content', token);
      }
    }
    return token;
  } catch {
    // Fallback to meta tag / cookie below; do not break the request flow.
    return null;
  }
}

function readCsrfFromDom(): string | null {
  const meta = document.querySelector('meta[name="csrf-token"]');
  if (meta) {
    return meta.getAttribute('content');
  }
  for (const cookie of document.cookie.split(';')) {
    const [name, value] = cookie.trim().split('=');
    if (name === 'drill.csrf.token') {
      return decodeURIComponent(value);
    }
  }
  return null;
}

async function getCsrfToken(): Promise<string | null> {
  if (csrfTokenPromise === null) {
    csrfTokenPromise = fetchCsrfToken();
  }
  const fetched = await csrfTokenPromise;
  return fetched ?? readCsrfFromDom();
}

/**
 * Force-refresh the cached CSRF token. Call after login or session changes.
 */
export function invalidateCsrfToken(): void {
  csrfTokenPromise = null;
}

/**
 * Resolve the current CSRF token. Used by non-axios callers (SSE streams,
 * direct fetch calls) that need to attach the header themselves.
 */
export async function resolveCsrfToken(): Promise<string | null> {
  return getCsrfToken();
}

/**
 * Warm the CSRF token cache at app boot so the first mutating request doesn't
 * pay the round-trip. Safe to call multiple times — subsequent calls reuse the
 * cached promise.
 */
export function prefetchCsrfToken(): Promise<string | null> {
  if (csrfTokenPromise === null) {
    csrfTokenPromise = fetchCsrfToken();
  }
  return csrfTokenPromise;
}

apiClient.interceptors.request.use(async (config) => {
  const method = config.method?.toLowerCase() ?? '';
  if (MUTATING_METHODS.has(method)) {
    const token = await getCsrfToken();
    if (token) {
      config.headers[CSRF_HEADER] = token;
    }
  }
  return config;
});

apiClient.interceptors.response.use(
  (response) => response,
  (error: AxiosError) => {
    if (error.response?.status === 401) {
      const redirect = encodeURIComponent(window.location.pathname + window.location.search);
      window.location.href = `/mainLogin?redirect=${redirect}`;
    }
    return Promise.reject(error);
  },
);

export default apiClient;

export function getErrorMessage(error: unknown): string {
  if (axios.isAxiosError(error)) {
    const axiosError = error as AxiosError<{ message?: string; error?: string }>;
    if (axiosError.response?.data?.message) {
      return axiosError.response.data.message;
    }
    if (axiosError.response?.data?.error) {
      return axiosError.response.data.error;
    }
    if (axiosError.message) {
      return axiosError.message;
    }
  }
  if (error instanceof Error) {
    return error.message;
  }
  return 'An unknown error occurred';
}
