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
import { buildSessionizeSql, isPacketCaptureFormat } from './sessionize';

describe('isPacketCaptureFormat', () => {
  it('accepts pcap and pcapng in any case', () => {
    expect(isPacketCaptureFormat('pcap')).toBe(true);
    expect(isPacketCaptureFormat('PCAPNG')).toBe(true);
  });

  it('rejects other formats and missing values', () => {
    expect(isPacketCaptureFormat('csv')).toBe(false);
    expect(isPacketCaptureFormat(undefined)).toBe(false);
  });
});

describe('buildSessionizeSql', () => {
  it('sessionizes a file through a table function, always with SELECT *', () => {
    expect(buildSessionizeSql('file:dfs.tmp:caps/traffic.pcapng')).toEqual({
      sql: "SELECT *\nFROM table(dfs.`tmp`.`caps/traffic.pcapng` (type => 'pcap', sessionizeTCPStreams => true))\nLIMIT 100",
      tabName: 'traffic.pcapng sessions',
    });
  });

  it('sessionizes a folder of captures', () => {
    expect(buildSessionizeSql('dir:dfs:captures/2024')).toEqual({
      sql: "SELECT *\nFROM table(dfs.`captures/2024` (type => 'pcap', sessionizeTCPStreams => true))\nLIMIT 100",
      tabName: '2024 sessions',
    });
  });

  it('returns null for keys that are not files or folders', () => {
    expect(buildSessionizeSql('schema:dfs.tmp')).toBeNull();
    expect(buildSessionizeSql('table:dfs.tmp:t')).toBeNull();
  });
});
