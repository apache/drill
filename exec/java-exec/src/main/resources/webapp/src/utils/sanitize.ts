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

/**
 * Sanitize a URL for use in an img src attribute.
 * Only allows http:, https:, data:image/, and relative paths.
 * Returns empty string for dangerous protocols like javascript: or vbscript:.
 */
export function sanitizeImageUrl(url: string): string {
  if (!url) {
    return '';
  }
  const trimmed = url.trim();
  if (trimmed.startsWith('/') || trimmed.startsWith('https://') || trimmed.startsWith('http://')) {
    return trimmed;
  }
  if (trimmed.startsWith('data:image/')) {
    return trimmed;
  }
  // Block javascript:, vbscript:, and other protocol-based URLs
  if (/^[a-z][a-z0-9+.-]*:/i.test(trimmed)) {
    return '';
  }
  return trimmed;
}
