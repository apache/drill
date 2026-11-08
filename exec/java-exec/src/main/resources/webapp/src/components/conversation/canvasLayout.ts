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

import { useSyncExternalStore } from 'react';

/**
 * Below this the page has no room to show the canvas beside the chat, and index.css hides
 * `.conversations-canvas`. CSS cannot read this constant, so the same number sits in that
 * media query: change them together.
 */
export const CANVAS_HIDDEN_QUERY = '(max-width: 1100px)';

function subscribe(onChange: () => void): () => void {
  const list = typeof window.matchMedia === 'function' ? window.matchMedia(CANVAS_HIDDEN_QUERY) : null;
  list?.addEventListener('change', onChange);
  return () => list?.removeEventListener('change', onChange);
}

function getSnapshot(): boolean {
  return typeof window.matchMedia === 'function' && window.matchMedia(CANVAS_HIDDEN_QUERY).matches;
}

/** True while the canvas is hidden by the layout, so nothing may send the user to it. */
export function useCanvasHidden(): boolean {
  return useSyncExternalStore(subscribe, getSnapshot, () => false);
}
