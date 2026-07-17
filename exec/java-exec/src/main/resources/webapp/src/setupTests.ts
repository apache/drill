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
