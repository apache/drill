/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.hadoop.util;

import java.io.IOException;
import java.io.OutputStream;
import java.util.BitSet;
import java.util.LinkedList;
import java.util.List;

/**
 *  Wrapper class around a {@link BytesArray} with 'awareness' around the underlying content.
 *  Considers each addition an entry and allows removal of specific entries (and by that skipping their backing content).
 *  Meant to be used as a buffer that is first filled, then emptied (in chunks) then cleaned-up.
 */
public class TrackingBytesArray implements ByteSequence {

    private static class Entry {
        final int offset;
        final int length;
        final int initialPosition;

        Entry(int offset, int length, int initialPosition) {
            this.offset = offset;
            this.length = length;
            this.initialPosition = initialPosition;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + length;
            result = prime * result + offset;
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            Entry other = (Entry) obj;
            if (length != other.length)
                return false;
            if (offset != other.offset)
                return false;
            return true;
        }
    }

    private final BytesArray data;
    private int maxEntries = 0;
    private int size = 0;
    private List<Entry> entries = new LinkedList<TrackingBytesArray.Entry>();

    public TrackingBytesArray(BytesArray data) {
        this.data = data;
    }

    public void copyFrom(BytesArray from) {
        addEntry(from.size);
        from.copyTo(data);
    }

    public void copyFrom(BytesRef from) {
        addEntry(from.length());
        from.copyTo(data);
    }

    public int length() {
        return size;
    }

    public int entries() {
        return entries.size();
    }

    public BitSet leftoversPosition() {
        BitSet bitSet = new BitSet(maxEntries);
        for (Entry entry : entries) {
            bitSet.set(entry.initialPosition);
        }

        return bitSet;
    }

    private void addEntry(int length) {
        // implied offset - data.size
        entries.add(new Entry(data.size, length, entries.size()));
        size += length;
        maxEntries = size;
    }

    public void remove(int index) {
        Entry entry = entries.remove(index);
        size -= entry.length;
    }

    public int length(int index) {
        return entries.get(index).length;
    }

    public void writeTo(OutputStream out) throws IOException {
        if (size == 0) {
            return;
        }

        for (Entry entry : entries) {
            out.write(data.bytes, entry.offset, entry.length);
        }
        out.flush();
    }

    public void reset() {
        size = 0;
        maxEntries = 0;
        entries.clear();
        data.reset();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder((int) length());
        for (Entry entry : entries) {
            sb.append(new String(data.bytes, entry.offset, entry.length, StringUtils.UTF_8));
        }
        return sb.toString();
    }
}