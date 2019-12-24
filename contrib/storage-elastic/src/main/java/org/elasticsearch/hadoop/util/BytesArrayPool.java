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
import java.util.ArrayList;
import java.util.List;

/**
 * Basic class acting as a pool of {@link BytesArray}. The goal here is to reuse the arrays even when dealing with a unknown/dynamic list of "bytes" and reuse them across calls.
 */
public class BytesArrayPool implements ByteSequence {

    private final List<BytesArray> pool = new ArrayList<BytesArray>();
    private int inUse = 0;

    @Override
    public int length() {
        int size = 0;
        for (int i = 0; i < inUse; i++) {
            size += pool.get(inUse).length();
        }
        return size;
    }

    @Override
    public void writeTo(OutputStream out) throws IOException {
        throw new UnsupportedOperationException();
    }

    public void reset() {
        for (BytesArray ba : pool) {
            ba.reset();
        }
        inUse = 0;
    }

    public BytesArray get() {
        if (inUse < pool.size() - 1) {
            return pool.get(inUse++);
        }
        else {
            BytesArray ba = new BytesArray(64);
            pool.add(ba);
            inUse++;
            return ba;
        }
    }

    public List<BytesArray> inUse() {
        return pool;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (BytesArray ba : pool) {
            sb.append(ba.toString());
            sb.append(";");
        }
        return sb.toString();
    }
}
