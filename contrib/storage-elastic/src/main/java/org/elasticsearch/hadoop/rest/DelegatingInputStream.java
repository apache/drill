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
package org.elasticsearch.hadoop.rest;

import java.io.IOException;
import java.io.InputStream;

import org.elasticsearch.hadoop.rest.stats.Stats;
import org.elasticsearch.hadoop.rest.stats.StatsAware;

public class DelegatingInputStream extends InputStream implements StatsAware {

    private final InputStream delegate;
    private final Stats stats = new Stats();

    public DelegatingInputStream(InputStream delegate) {
        this.delegate = delegate;
    }

    @Override
    public int read() throws IOException {
        return delegate != null ? delegate.read() : -1;
    }

    @Override
    public int hashCode() {
        return delegate != null ? delegate.hashCode() : 0;
    }

    @Override
    public int read(byte[] b) throws IOException {
        int result = (delegate != null ? delegate.read(b) : -1);
        if (result > 0) {
            stats.bytesReceived += result;
        }
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        return delegate != null ? delegate.equals(obj) : (obj == null ? true : false);
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        int result = (delegate != null ? delegate.read(b, off, len) : -1);
        if (result > 0) {
            stats.bytesReceived += result;
        }
        return result;
    }

    @Override
    public long skip(long n) throws IOException {
        return delegate != null ? delegate.skip(n) : -1;
    }

    @Override
    public int available() throws IOException {
        return delegate != null ? delegate.available() : 0;
    }

    @Override
    public String toString() {
        return delegate != null ? delegate.toString() : "null";
    }

    @Override
    public void close() throws IOException {
        if (delegate != null) {
            delegate.close();
        }
    }

    @Override
    public void mark(int readlimit) {
        if (delegate != null) {
            delegate.mark(readlimit);
        }
    }

    @Override
    public void reset() throws IOException {
        if (delegate != null) {
            delegate.reset();
        }
    }

    @Override
    public boolean markSupported() {
        return delegate != null ? delegate.markSupported() : false;
    }

    public boolean isNull() {
        return delegate == null;
    }

    public InputStream delegate() {
        return delegate;
    }

    @Override
    public Stats stats() {
        return stats;
    }
}