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
import { analyzeSeed, errorSeed } from './logSeeds';

const lines = Array.from({ length: 300 }, (_, i) => `line ${i + 1}`);

describe('analyzeSeed', () => {
  it('sends the last 200 lines in log-analysis mode for that file', () => {
    const seed = analyzeSeed('drillbit.log', lines);
    expect(seed.context).toMatchObject({
      feature: 'log_analysis', logAnalysisMode: true, logFileName: 'drillbit.log',
    });
    expect(seed.context!.logLines).toHaveLength(200);
    expect(seed.context!.logLines![0]).toBe('line 101');
    expect(seed.context!.logLines![199]).toBe('line 300');
  });
});

describe('errorSeed', () => {
  it('quotes the clicked line and sends five lines either side of it', () => {
    const seed = errorSeed('drillbit.log', lines, 50);
    expect(seed.prompt).toContain('line 50');
    expect(seed.prompt).not.toContain('line 51');
    expect(seed.context!.logLines).toEqual(lines.slice(44, 55));
  });

  it('clamps at the start and end of the file', () => {
    expect(errorSeed('f', lines, 2).context!.logLines![0]).toBe('line 1');
    expect(errorSeed('f', lines, 300).context!.logLines!.at(-1)).toBe('line 300');
  });
});
