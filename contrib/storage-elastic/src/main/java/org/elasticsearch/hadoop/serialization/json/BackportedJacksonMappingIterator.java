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
package org.elasticsearch.hadoop.serialization.json;

import java.io.IOException;
import java.util.Iterator;

import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonStreamContext;
import org.codehaus.jackson.JsonToken;
import org.codehaus.jackson.map.DeserializationContext;
import org.codehaus.jackson.map.JsonDeserializer;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.RuntimeJsonMappingException;
import org.codehaus.jackson.type.JavaType;

/**
 * Backported class from Jackson 1.8.8 for Jackson 1.5.2
 *
 * Iterator exposed by {@link ObjectMapper} when binding sequence of
 * objects. Extension is done to allow more convenient exposing of
 * {@link IOException} (which basic {@link Iterator} does not expose)
 *
 * @since 1.8
 */
class BackportedJacksonMappingIterator<T> implements Iterator<T> {
    protected final static BackportedJacksonMappingIterator<?> EMPTY_ITERATOR = new BackportedJacksonMappingIterator<Object>(null, null, null, null);

    protected final JavaType _type;

    protected final DeserializationContext _context;

    protected final JsonDeserializer<T> _deserializer;

    protected final JsonParser _parser;

    @SuppressWarnings("unchecked")
    protected BackportedJacksonMappingIterator(JavaType type, JsonParser jp, DeserializationContext ctxt, JsonDeserializer<?> deser) {
        _type = type;
        _parser = jp;
        _context = ctxt;
        _deserializer = (JsonDeserializer<T>) deser;

        /* One more thing: if we are at START_ARRAY (but NOT root-level
         * one!), advance to next token (to allow matching END_ARRAY)
         */
        if (jp != null && jp.getCurrentToken() == JsonToken.START_ARRAY) {
            JsonStreamContext sc = jp.getParsingContext();
            // safest way to skip current token is to clear it (so we'll advance soon)
            if (!sc.inRoot()) {
                jp.clearCurrentToken();
            }
        }
    }

    @SuppressWarnings("unchecked")
    protected static <T> BackportedJacksonMappingIterator<T> emptyIterator() {
        return (BackportedJacksonMappingIterator<T>) EMPTY_ITERATOR;
    }

    /*
    /**********************************************************
    /* Basic iterator impl
    /**********************************************************
     */

    @Override
    public boolean hasNext() {
        try {
            return hasNextValue();
        } catch (JsonMappingException e) {
            throw new RuntimeJsonMappingException(e.getMessage(), e);
        } catch (IOException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    @Override
    public T next() {
        try {
            return nextValue();
        } catch (JsonMappingException e) {
            throw new RuntimeJsonMappingException(e.getMessage(), e);
        } catch (IOException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }

    /*
    /**********************************************************
    /* Extended API
    /**********************************************************
     */

    /**
     * Equivalent of {@link #next} but one that may throw checked
     * exceptions from Jackson due to invalid input.
     */
    public boolean hasNextValue() throws IOException {
        if (_parser == null) {
            return false;
        }
        JsonToken t = _parser.getCurrentToken();
        if (t == null) { // un-initialized or cleared; find next
            t = _parser.nextToken();
            // If EOF, no more
            if (t == null) {
                _parser.close();
                return false;
            }
            // And similarly if we hit END_ARRAY; except that we won't close parser
            if (t == JsonToken.END_ARRAY) {
                return false;
            }
        }
        return true;
    }

    public T nextValue() throws IOException {
        T result = _deserializer.deserialize(_parser, _context);
        // Need to consume the token too
        _parser.clearCurrentToken();
        return result;
    }
}