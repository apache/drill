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
import Markdown from 'react-markdown';
import type { Components } from 'react-markdown';
import remarkGfm from 'remark-gfm';
import rehypeRaw from 'rehype-raw';
import rehypeSanitize from 'rehype-sanitize';

interface MarkdownViewProps {
  children: string;
  /** Render raw HTML embedded in the markdown. Always sanitized. */
  allowHtml?: boolean;
  components?: Components;
}

/**
 * Markdown renderer for AI- and user-authored content.
 *
 * Use this instead of react-markdown directly: it enables GFM (tables,
 * strikethrough, task lists) and, when raw HTML is allowed, always pairs
 * rehype-raw with rehype-sanitize. react-markdown does no sanitizing of its
 * own, so rehype-raw without it is an XSS hole.
 */
export default function MarkdownView({ children, allowHtml, components }: MarkdownViewProps) {
  return (
    <Markdown
      remarkPlugins={[remarkGfm]}
      rehypePlugins={allowHtml ? [rehypeRaw, rehypeSanitize] : []}
      components={components}
    >
      {children}
    </Markdown>
  );
}
