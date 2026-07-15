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
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { FEATURE_LABEL, featureLabel } from './aiFeatures';

const SRC_ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');

/** Slugs recorded by the server, which never appear as `feature: '...'` in this tree. */
const SERVER_SIDE_FEATURES = ['config_test', 'transpile', 'prospector_chat'];

function sourceFiles(dir: string): string[] {
  const out: string[] = [];
  for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
    const full = path.join(dir, entry.name);
    if (entry.isDirectory()) {
      out.push(...sourceFiles(full));
    } else if (/\.tsx?$/.test(entry.name) && !/\.test\.tsx?$/.test(entry.name)) {
      out.push(full);
    }
  }
  return out;
}

/**
 * Every `feature: 'slug'` literal in the UI source — the set of slugs the browser
 * can attach to a chat request, and therefore the set the server can record.
 */
function emittedFeatures(): Set<string> {
  const found = new Set<string>();
  for (const file of sourceFiles(SRC_ROOT)) {
    const text = fs.readFileSync(file, 'utf8');
    for (const m of text.matchAll(/\bfeature:\s*'([a-z0-9_]+)'/g)) {
      found.add(m[1]);
    }
  }
  return found;
}

describe('FEATURE_LABEL', () => {
  /**
   * The dashboard builds its feature filter from FEATURE_LABEL's keys, so a slug that
   * is emitted but unlabelled is invisible in the filter and renders as a raw slug.
   * This guards against that drifting apart as new AI surfaces are added.
   */
  it('labels every feature slug emitted by the UI', () => {
    const missing = [...emittedFeatures()].filter((f) => !(f in FEATURE_LABEL)).sort();
    expect(missing).toEqual([]);
  });

  it('labels every server-recorded feature slug', () => {
    const missing = SERVER_SIDE_FEATURES.filter((f) => !(f in FEATURE_LABEL));
    expect(missing).toEqual([]);
  });

  /**
   * A label with no emitter is dead UI: it offers the admin a filter that can never
   * match an event. Fails loudly if a surface is removed without dropping its label.
   */
  it('has no labels for features nothing emits', () => {
    const live = new Set([...emittedFeatures(), ...SERVER_SIDE_FEATURES]);
    const orphans = Object.keys(FEATURE_LABEL).filter((f) => !live.has(f)).sort();
    expect(orphans).toEqual([]);
  });

  it('finds the slugs it claims to scan for', () => {
    // Guards the scanner itself: a regex that matches nothing would make the
    // drift tests above pass vacuously.
    expect(emittedFeatures().has('sql_lab_chat')).toBe(true);
    expect(emittedFeatures().size).toBeGreaterThanOrEqual(14);
  });

  it('falls back to the raw slug for unknown features', () => {
    expect(featureLabel('sql_lab_chat')).toBe('SQL Lab chat');
    expect(featureLabel('something_new')).toBe('something_new');
  });
});
