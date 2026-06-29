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
import type { VisualizationConfig } from '../types';

/**
 * One column slot referenced by a visualization config (e.g. xAxis, a metric,
 * a dimension). Used to tell the user *which* mapping is broken when the
 * underlying SQL no longer returns a referenced column.
 */
export interface ColumnSlot {
  /** Human-readable slot label, e.g. "x-axis", "metric", "dimension". */
  slot: string;
  /** The column name that slot is currently bound to. */
  column: string;
}

/**
 * Flatten a VisualizationConfig into the (slot, column) pairs it references.
 * Empty/undefined fields are skipped. Order mirrors how a user thinks about
 * a chart: axes first, then series.
 */
export function listConfigColumnRefs(config: VisualizationConfig | undefined): ColumnSlot[] {
  if (!config) {
    return [];
  }
  const refs: ColumnSlot[] = [];
  if (config.xAxis) {
    refs.push({ slot: 'x-axis', column: config.xAxis });
  }
  if (config.yAxis) {
    refs.push({ slot: 'y-axis', column: config.yAxis });
  }
  (config.metrics ?? []).forEach((m) => {
    if (m) {
      refs.push({ slot: 'metric', column: m });
    }
  });
  (config.dimensions ?? []).forEach((d) => {
    if (d) {
      refs.push({ slot: 'dimension', column: d });
    }
  });
  return refs;
}

/**
 * Compare a viz config against the columns the underlying query actually
 * returns. Returns the slots whose column is missing — what the user has to
 * decide about before saving. An empty array means the chart will still render
 * with the new SQL.
 */
export function findMissingColumnRefs(
  config: VisualizationConfig | undefined,
  resultColumns: readonly string[] | undefined,
): ColumnSlot[] {
  if (!config || !resultColumns || resultColumns.length === 0) {
    return [];
  }
  const present = new Set(resultColumns);
  return listConfigColumnRefs(config).filter((ref) => !present.has(ref.column));
}

/**
 * Group missing refs by column so the user sees one entry per missing column
 * with the list of slots it powers, rather than one entry per (column, slot).
 * Useful for the warning UI.
 */
export interface MissingColumnGroup {
  column: string;
  slots: string[];
}

export function groupMissingByColumn(missing: ColumnSlot[]): MissingColumnGroup[] {
  const map = new Map<string, MissingColumnGroup>();
  for (const ref of missing) {
    const existing = map.get(ref.column);
    if (existing) {
      if (!existing.slots.includes(ref.slot)) {
        existing.slots.push(ref.slot);
      }
    } else {
      map.set(ref.column, { column: ref.column, slots: [ref.slot] });
    }
  }
  return Array.from(map.values());
}
