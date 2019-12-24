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

import org.apache.commons.httpclient.HostConfiguration;
import org.apache.commons.httpclient.HttpHost;
import org.apache.commons.httpclient.protocol.Protocol;

class ProtocolAwareHostConfiguration extends HostConfiguration {
    public ProtocolAwareHostConfiguration(HostConfiguration hostConfiguration) {
        super(hostConfiguration);
    }

    public Object clone() {
        return new ProtocolAwareHostConfiguration(this);
    }

    public synchronized void setHost(String host, int port, String scheme) {
        setHost(new HttpHost(host, port, keepProtocol(host, port, scheme)));
    }

    protected Protocol keepProtocol(String host, int port, String scheme) {
        final Protocol oldProtocol = getProtocol();
        if (oldProtocol != null) {
            final String oldScheme = oldProtocol.getScheme();
            if (oldScheme == scheme || (oldScheme != null && oldScheme.equalsIgnoreCase(scheme))) {
                return oldProtocol;
            }
        }
        return Protocol.getProtocol(scheme);
    }
}