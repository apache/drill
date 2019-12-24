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

import org.elasticsearch.hadoop.util.ByteSequence;


public class SimpleRequest implements Request {

    private final Method method;
    private final CharSequence uri;
    private final CharSequence path;
    private final CharSequence params;
    private final ByteSequence body;

    public SimpleRequest(Method method, CharSequence uri, CharSequence path) {
        this(method, uri, path, null, null);
    }

    public SimpleRequest(Method method, CharSequence uri, CharSequence path, CharSequence params) {
        this(method, uri, path, params, null);
    }

    public SimpleRequest(Method method, CharSequence uri, CharSequence path, ByteSequence body) {
        this(method, uri, path, null, body);
    }

    public SimpleRequest(Method method, CharSequence uri, CharSequence path, CharSequence params, ByteSequence body) {
        this.method = method;
        this.uri = uri;
        this.path = path;
        this.params = params;
        this.body = body;
    }

    @Override
    public Method method() {
        return method;
    }

    @Override
    public CharSequence uri() {
        return uri;
    }

    @Override
    public CharSequence path() {
        return path;
    }

    @Override
    public CharSequence params() {
        return params;
    }

    @Override
    public ByteSequence body() {
        return body;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder(method.name());
        sb.append("@");
        sb.append(uri);
        sb.append("/");
        sb.append(path);
        if (params != null) {
            sb.append("?");
            sb.append(params);
        }
        return sb.toString();
    }
}