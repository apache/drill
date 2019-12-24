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

import java.util.Collection;
import java.util.Collections;
import java.util.Map;

/**
 * Exception indicating that due to errors, all available nodes have been processed and no
 * other nodes are left for retrying.
 */
public class EsHadoopNoNodesLeftException extends EsHadoopTransportException {

    private final Map<String, Throwable> nodesUsed;

    public EsHadoopNoNodesLeftException() {
        super(initMessage(Collections.<String, Throwable> emptyMap()));
        nodesUsed = Collections.emptyMap();
    }

    public EsHadoopNoNodesLeftException(Map<String, Throwable> nodesUsed) {
        super(initMessage(nodesUsed));
        this.nodesUsed = nodesUsed;
    }

    private static String initMessage(Map<String, Throwable> nodesUsed) {
        return String.format("Connection error (check network and/or proxy settings)- all nodes failed; tried [%s] ", nodesUsed.keySet());
    }

    public Collection<String> nodesUsed() {
        return nodesUsed.keySet();
    }
}
