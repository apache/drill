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
import { formatSchema } from './sql';

/** Extensions read by Drill's pcap format plugin (type 'pcap' covers both). */
const PACKET_CAPTURE_FORMATS = new Set(['pcap', 'pcapng']);

/** True if a file extension or folder data format is a packet capture. */
export function isPacketCaptureFormat(format: string | undefined): boolean {
  return format !== undefined && PACKET_CAPTURE_FORMATS.has(format.toLowerCase());
}

/**
 * A query returning one row per TCP session for a capture file or a folder of
 * captures, from its schema explorer key (file:<schema>:<path> or dir:<schema>:<path>).
 * Always SELECT *: sessions have different columns than packets.
 */
export function buildSessionizeSql(nodeKey: string): { sql: string; tabName: string } | null {
  const match = /^(file|dir):([^:]+):(.+)$/.exec(nodeKey);
  if (!match) {
    return null;
  }
  const [, , schema, path] = match;
  const source = `${formatSchema(schema)}.\`${path}\``;
  return {
    sql: `SELECT *\nFROM table(${source} (type => 'pcap', sessionizeTCPStreams => true))\nLIMIT 100`,
    tabName: `${path.split('/').pop()} sessions`,
  };
}
