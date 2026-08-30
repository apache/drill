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
import '@testing-library/jest-dom';

// Node 26 exposes a global `localStorage` accessor that returns undefined unless the
// process was started with --localstorage-file. Vitest's jsdom environment only copies a
// jsdom window property onto globalThis when the key is absent from the Node global (or is
// on its small hardcoded allowlist, which predates Node having Web Storage), so jsdom's
// working localStorage is skipped and Node's dead stub shadows it. sessionStorage is
// skipped the same way but survives because Node's own implementation works in memory.
// ponytail: in-memory shim rather than a vitest upgrade; delete once vitest's jsdom
// environment copies localStorage through (or once Node's global one works unflagged).
if (typeof globalThis.localStorage === 'undefined') {
  const store = new Map<string, string>();
  const storage: Storage = {
    get length() {
      return store.size;
    },
    key: (i: number) => Array.from(store.keys())[i] ?? null,
    getItem: (k: string) => (store.has(k) ? store.get(k)! : null),
    setItem: (k: string, v: string) => {
      store.set(String(k), String(v));
    },
    removeItem: (k: string) => {
      store.delete(k);
    },
    clear: () => {
      store.clear();
    },
  };
  Object.defineProperty(globalThis, 'localStorage', {
    value: storage,
    writable: true,
    configurable: true,
  });
}

// Mock matchMedia for antd components
Object.defineProperty(window, 'matchMedia', {
  writable: true,
  value: vi.fn().mockImplementation((query: string) => ({
    matches: false,
    media: query,
    onchange: null,
    addListener: vi.fn(),
    removeListener: vi.fn(),
    addEventListener: vi.fn(),
    removeEventListener: vi.fn(),
    dispatchEvent: vi.fn(),
  })),
});

// antd's popup alignment (rc-trigger/rc-align) calls getComputedStyle(el, pseudoElt) while
// positioning dropdowns. jsdom does not implement the pseudo-element form and logs a
// "Not implemented" virtual-console error on every call, flooding CI output. The previous
// shim passed pseudoElt straight through, so it suppressed nothing. Dropping the argument
// returns the element's own computed style — all antd needs — and silences the noise.
const originalGetComputedStyle = window.getComputedStyle;
window.getComputedStyle = (elt: Element) => originalGetComputedStyle(elt);

// Real Monaco cannot boot in jsdom (it probes document.queryCommandSupported and friends).
// Components import ./monaco for its loader side effect only; tests mock @monaco-editor/react.
vi.mock('./monaco', () => ({}));
