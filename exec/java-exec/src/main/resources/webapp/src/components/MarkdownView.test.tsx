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
import { render, screen } from '@testing-library/react';
import MarkdownView from './MarkdownView';

const TABLE = [
  '| Column | Description |',
  '|---|---|',
  '| Customer ID | Unique customer identifier |',
  '| Name | Full name |',
].join('\n');

describe('MarkdownView', () => {
  it('renders a GFM table as a real table', () => {
    const { container } = render(<MarkdownView>{TABLE}</MarkdownView>);
    expect(container.querySelector('table')).not.toBeNull();
    expect(container.querySelectorAll('tbody tr')).toHaveLength(2);
    expect(screen.getByText('Unique customer identifier')).toBeInTheDocument();
    // The pipes must not survive as literal text.
    expect(container.textContent).not.toContain('|---|');
  });

  it('strips dangerous raw HTML when html is allowed', () => {
    const { container } = render(
      <MarkdownView allowHtml>{'<img src=x onerror="alert(1)"> ok'}</MarkdownView>,
    );
    expect(container.querySelector('img')?.getAttribute('onerror')).toBeNull();
    expect(container.querySelector('script')).toBeNull();
  });
});
