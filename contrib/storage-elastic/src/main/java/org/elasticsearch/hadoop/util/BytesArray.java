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
import java.util.Arrays;

/**
 * Wrapper class around a bytes array so that it can be passed as reference even if the underlying array is modified.
 * Allows only a part of the array to be used (slicing).
 */
public class BytesArray implements ByteSequence {

    public static final byte[] EMPTY = new byte[0];

    byte[] bytes = EMPTY;
    int offset = 0;
    int size = 0;

    public BytesArray(int size) {
        this(new byte[size], 0, 0);
    }

    public BytesArray(byte[] data) {
        this(data, 0, data.length);
    }

    public BytesArray(byte[] data, int size) {
        this(data, 0, size);
    }

    public BytesArray(byte[] data, int offset, int size) {
        this.bytes = data;
        this.offset = offset;
        this.size = size;
    }

    public BytesArray(String source) {
        bytes(source);
    }

    public byte[] bytes() {
        return bytes;
    }

    public int offset() {
        return offset;
    }

    public int length() {
        return size;
    }

    public int capacity() {
        return bytes.length;
    }

    public int available() {
        return bytes.length - size;
    }

    public void bytes(byte[] array) {
        this.bytes = array;
        this.size = array.length;
        this.offset = 0;
    }

    public void bytes(byte[] array, int size) {
        this.bytes = array;
        this.size = size;
        this.offset = 0;
    }

    public void bytes(BytesArray ba) {
        this.bytes = ba.bytes;
        this.size = ba.size;
        this.offset = ba.offset;
    }

    public void bytes(String from) {
        size = 0;
        offset = 0;
        UnicodeUtil.UTF16toUTF8(from, 0, from.length(), this);
    }

    public void size(int size) {
        this.size = size;
    }

    public void offset(int offset) {
        this.offset = offset;
    }

    @Override
    public String toString() {
        return StringUtils.asUTFString(bytes, offset, size);
    }

    public void reset() {
        offset = 0;
        size = 0;
    }

    public void copyTo(BytesArray to) {
        to.add(bytes, offset, size);
    }

    public void add(int b) {
        int newcount = size + 1;
        checkSize(newcount);
        bytes[size] = (byte) b;
        size = newcount;
    }

    public void add(byte[] b) {
        if (b == null || b.length == 0) {
            return;
        }
        add(b, 0, b.length);
    }

    public void add(byte[] b, int off, int len) {
        if (len == 0) {
            return;
        }
        int newcount = size + len;
        checkSize(newcount);
        try {
            System.arraycopy(b, off, bytes, size, len);
        } catch (ArrayIndexOutOfBoundsException ex) {
            System.err.println(String.format("Copying array of size %d, content %s, off %d, len %d to bytes with len %d at offset %d", b.length, new BytesArray(b), off, len, bytes.length, size));
            throw ex;
        }
        size = newcount;
    }

    public void add(String string) {
        if (string == null) {
            return;
        }
        add(string.getBytes(StringUtils.UTF_8));
    }

    private void checkSize(int newcount) {
        if (newcount > bytes.length) {
            bytes = ArrayUtils.grow(bytes, newcount);
        }
    }

    public void writeTo(OutputStream out) throws IOException {
        out.write(bytes, offset, size);
        out.flush();
    }
}