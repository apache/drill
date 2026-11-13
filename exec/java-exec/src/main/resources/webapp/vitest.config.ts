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
import { defineConfig } from 'vitest/config';
import react from '@vitejs/plugin-react';

export default defineConfig({
  plugins: [react()],
  test: {
    environment: 'jsdom',
    globals: true,
    setupFiles: ['./src/setupTests.ts'],
    css: true,
    // These are integration-style tests: a page test mounts the router, antd and a lazily
    // imported page, and costs well over a second. Testing Library's default asyncUtilTimeout
    // is 1s, which is shorter than a single render under a loaded CI runner, so findBy* calls
    // were timing out and reporting "unable to find" for elements that do appear. Waiting
    // longer does not weaken an assertion: a behaviour that is actually wrong still fails,
    // it just fails five seconds later. testTimeout has to exceed asyncUtilTimeout, or a
    // test dies on the test deadline before the wait it is doing can report what it wanted.
    testTimeout: 30_000,
    hookTimeout: 30_000,
  },
});
