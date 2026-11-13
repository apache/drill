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
import { describe, it, expect, vi } from 'vitest';
import { render, screen, fireEvent, createEvent } from '@testing-library/react';
import ChatInput from './ChatInput';

function renderInput(onFileDropped?: (file: File) => void) {
  render(<ChatInput onSend={vi.fn()} onStop={vi.fn()} isStreaming={false} onFileDropped={onFileDropped} />);
  return screen.getByRole('textbox').closest('.prospector-chat-input') as HTMLElement;
}

function drop(target: HTMLElement, dataTransfer: { types: string[]; files: File[] }) {
  const event = createEvent.drop(target);
  Object.defineProperty(event, 'dataTransfer', { value: dataTransfer });
  fireEvent(target, event);
  return event;
}

describe('ChatInput file drop', () => {
  it('uploads a file dropped on the composer', () => {
    const onFileDropped = vi.fn();
    const target = renderInput(onFileDropped);
    const file = new File(['a,b'], 'sales.csv', { type: 'text/csv' });

    const event = drop(target, { types: ['Files'], files: [file] });

    expect(onFileDropped).toHaveBeenCalledWith(file);
    expect(event.defaultPrevented).toBe(true);
  });

  it('ignores a drop when there is no project to upload into', () => {
    const target = renderInput();
    const file = new File(['a,b'], 'sales.csv', { type: 'text/csv' });

    const event = drop(target, { types: ['Files'], files: [file] });

    expect(event.defaultPrevented).toBe(false);
  });

  it('ignores a drag that carries no files', () => {
    const onFileDropped = vi.fn();
    const target = renderInput(onFileDropped);

    drop(target, { types: ['text/plain'], files: [] });

    expect(onFileDropped).not.toHaveBeenCalled();
  });
});
