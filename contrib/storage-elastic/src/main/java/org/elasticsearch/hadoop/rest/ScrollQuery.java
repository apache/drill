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

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.elasticsearch.hadoop.EsHadoopIllegalStateException;
import org.elasticsearch.hadoop.rest.stats.Stats;
import org.elasticsearch.hadoop.rest.stats.StatsAware;
import org.elasticsearch.hadoop.serialization.ScrollReader;
import org.elasticsearch.hadoop.serialization.ScrollReader.Scroll;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * Result streaming data from a ElasticSearch query using the scan/scroll. Performs batching underneath to retrieve data in chunks.
 */
public class ScrollQuery implements Iterator<JsonNode>, Closeable, StatsAware {

    private RestRepository repository;
    private String scrollId;
    private  Iterator<JsonNode> iterator =  null;
    private boolean finished = false;

    private long read = 0;
    // how many docs to read - in most cases, all the docs that match
    private long size;

    private final ScrollReader reader;

    private final Stats stats = new Stats();

    private boolean closed = false;

    ScrollQuery(RestRepository client, String scrollId, long size, ScrollReader reader) {
        this.repository = client;
        this.scrollId = scrollId;
        this.size = size;
        this.reader = reader;
    }

    @Override
    public void close() {
        if (!closed) {
            closed = true;
            finished = true;
            // typically the scroll is closed after it is consumed so this will trigger a 404
            // however we're closing it either way
            repository.getRestClient().deleteScroll(scrollId);
        }
    }

    @Override
    public boolean hasNext() {
        if (finished)
            return false;

        if (iterator == null || !iterator.hasNext()  ) {
            if (read >= size) {
                finished = true;
                return false;
            }

            try {
                Scroll scroll = repository.scroll(scrollId, reader);
                scrollId = scroll.getScrollId();
                iterator = scroll.getIterator();
                read += scroll.getTotalHits();
                stats.docsReceived += scroll.getTotalHits();
            } catch (IOException ex) {
                throw new EsHadoopIllegalStateException("Cannot retrieve scroll [" + scrollId + "]", ex);
            }
       

            if (!iterator.hasNext()) {
                finished = true;
                return false;
            }
        }

        return true;
    }

    public long getSize() {
        return size;
    }

    public long getRead() {
        return read;
    }

    @Override
    public JsonNode  next() {
        if (!hasNext()) {
            throw new NoSuchElementException("No more documents available");
        }
        return iterator.next();
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("read-only operator");
    }

    @Override
    public Stats stats() {
        // there's no need to do aggregation
        return new Stats(stats);
    }

    public RestRepository repository() {
        return repository;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("ScrollQuery [scrollId=").append(scrollId).append("]");
        return builder.toString();
    }
}