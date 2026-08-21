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
import { describe, it, expect } from 'vitest';
import { sanitizeImageUrl } from './sanitize';

describe('sanitizeImageUrl', () => {
  it('returns empty string for empty input', () => {
    expect(sanitizeImageUrl('')).toBe('');
  });

  it('allows https URLs', () => {
    expect(sanitizeImageUrl('https://example.com/photo.png')).toBe('https://example.com/photo.png');
  });

  it('allows http URLs', () => {
    expect(sanitizeImageUrl('http://example.com/photo.png')).toBe('http://example.com/photo.png');
  });

  it('allows relative paths starting with /', () => {
    expect(sanitizeImageUrl('/api/v1/dashboards/images/abc.png')).toBe('/api/v1/dashboards/images/abc.png');
  });

  it('allows data:image/ URLs', () => {
    expect(sanitizeImageUrl('data:image/png;base64,abc123')).toBe('data:image/png;base64,abc123');
  });

  it('blocks javascript: protocol', () => {
    expect(sanitizeImageUrl('javascript:alert(1)')).toBe('');
  });

  it('blocks JavaScript: protocol (case-insensitive)', () => {
    expect(sanitizeImageUrl('JavaScript:alert(1)')).toBe('');
  });

  it('blocks vbscript: protocol', () => {
    expect(sanitizeImageUrl('vbscript:MsgBox("xss")')).toBe('');
  });

  it('blocks data: URLs that are not images', () => {
    expect(sanitizeImageUrl('data:text/html,<script>alert(1)</script>')).toBe('');
  });

  it('trims whitespace', () => {
    expect(sanitizeImageUrl('  https://example.com/photo.png  ')).toBe('https://example.com/photo.png');
  });

  it('allows plain filenames without protocol', () => {
    expect(sanitizeImageUrl('photo.png')).toBe('photo.png');
  });
});
