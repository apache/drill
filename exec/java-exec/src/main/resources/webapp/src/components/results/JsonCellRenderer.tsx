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
import { useState, useCallback } from 'react';
import { Modal } from 'antd';
import { ExpandOutlined } from '@ant-design/icons';

interface JsonCellRendererProps {
  value: unknown;
}

/**
 * Tries to parse a string as JSON. Returns the parsed object if valid, or null.
 */
function tryParseJson(value: string): unknown | null {
  if (!value.startsWith('{') && !value.startsWith('[')) {
    return null;
  }
  try {
    return JSON.parse(value);
  } catch {
    return null;
  }
}

/**
 * Renders a JSON value with syntax highlighting.
 */
function highlightJson(json: string): JSX.Element[] {
  const elements: JSX.Element[] = [];
  // Match JSON tokens: strings, numbers, booleans, null, braces/brackets, colons, commas
  const tokenRegex = /("(?:[^"\\]|\\.)*")\s*:|("(?:[^"\\]|\\.)*")|(-?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?)|(\btrue\b|\bfalse\b)|(\bnull\b)|([{}[\]:,])/g;
  let match: RegExpExecArray | null;
  let lastIndex = 0;

  while ((match = tokenRegex.exec(json)) !== null) {
    // Add any whitespace/newlines between tokens
    if (match.index > lastIndex) {
      elements.push(
        <span key={`ws-${lastIndex}`}>{json.slice(lastIndex, match.index)}</span>
      );
    }

    if (match[1]) {
      // Key
      elements.push(
        <span key={`k-${match.index}`}>
          <span style={{ color: 'var(--json-key-color, #881391)' }}>{match[1]}</span>:
        </span>
      );
    } else if (match[2]) {
      // String value
      elements.push(
        <span key={`s-${match.index}`} style={{ color: 'var(--json-string-color, #1a1aa6)' }}>{match[2]}</span>
      );
    } else if (match[3]) {
      // Number
      elements.push(
        <span key={`n-${match.index}`} style={{ color: 'var(--json-number-color, #098658)' }}>{match[3]}</span>
      );
    } else if (match[4]) {
      // Boolean
      elements.push(
        <span key={`b-${match.index}`} style={{ color: 'var(--json-bool-color, #0451a5)' }}>{match[4]}</span>
      );
    } else if (match[5]) {
      // Null
      elements.push(
        <span key={`x-${match.index}`} style={{ color: 'var(--color-text-tertiary, #999)' }}>{match[5]}</span>
      );
    } else if (match[6]) {
      // Structural characters
      elements.push(
        <span key={`p-${match.index}`} style={{ color: 'var(--color-text, #333)' }}>{match[6]}</span>
      );
    }

    lastIndex = match.index + match[0].length;
  }

  // Remaining text
  if (lastIndex < json.length) {
    elements.push(<span key={`end-${lastIndex}`}>{json.slice(lastIndex)}</span>);
  }

  return elements;
}

export default function JsonCellRenderer({ value }: JsonCellRendererProps) {
  const [modalOpen, setModalOpen] = useState(false);

  const handleOpen = useCallback((e: React.MouseEvent) => {
    e.stopPropagation();
    setModalOpen(true);
  }, []);

  if (value == null) {
    return <span></span>;
  }

  const strValue = typeof value === 'object' ? JSON.stringify(value) : String(value);

  // Check if value is JSON (object or array)
  const parsed = typeof value === 'object' ? value : tryParseJson(strValue);
  if (parsed === null) {
    return <span>{strValue}</span>;
  }

  const formatted = JSON.stringify(parsed, null, 2);

  return (
    <>
      <span className="json-cell">
        <span className="json-cell-preview">{strValue}</span>
        <span
          className="json-cell-expand"
          onClick={handleOpen}
          title="Expand JSON"
        >
          <ExpandOutlined />
        </span>
      </span>

      <Modal
        title="JSON Value"
        open={modalOpen}
        onCancel={() => setModalOpen(false)}
        footer={null}
        width={640}
        styles={{ body: { maxHeight: '70vh', overflow: 'auto' } }}
      >
        <pre className="json-viewer">
          {highlightJson(formatted)}
        </pre>
      </Modal>
    </>
  );
}
