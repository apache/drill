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
package org.elasticsearch.hadoop.rest.commonshttp;

import org.apache.commons.httpclient.protocol.Protocol;
import org.apache.commons.httpclient.protocol.ProtocolSocketFactory;

/**
 * Class used to make sure wrapped protocol object is behaving like the original one.
 * Otherwise this leads to connections not being closed (as they are considered different).
 */
class DelegatedProtocol extends Protocol {

    private final Protocol original;

    DelegatedProtocol(ProtocolSocketFactory factory, Protocol original, String scheme, int port) {
        super(scheme, factory, port);
        this.original = original;
    }

    public boolean equals(Object obj) {
        return (obj instanceof DelegatedProtocol ? true : original.equals(obj));
    }

    public int hashCode() {
        return original.hashCode();
    }
}
