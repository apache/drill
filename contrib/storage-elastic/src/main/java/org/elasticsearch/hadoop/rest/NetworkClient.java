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
import java.net.BindException;
import java.net.NoRouteToHostException;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.elasticsearch.hadoop.EsHadoopException;
import org.elasticsearch.hadoop.EsHadoopIllegalStateException;
import org.elasticsearch.hadoop.cfg.Settings;
import org.elasticsearch.hadoop.rest.commonshttp.CommonsHttpTransport;
import org.elasticsearch.hadoop.rest.stats.Stats;
import org.elasticsearch.hadoop.rest.stats.StatsAware;
import org.elasticsearch.hadoop.util.Assert;
import org.elasticsearch.hadoop.util.ByteSequence;
import org.elasticsearch.hadoop.util.SettingsUtils;


public class NetworkClient implements StatsAware, Closeable {
    private static Log log = LogFactory.getLog(NetworkClient.class);

    private final Settings settings;
    private final List<String> nodes;
    private final Map<String, Throwable> failedNodes = new LinkedHashMap<String, Throwable>();

    private Transport currentTransport;
    private String currentNode;
    private int nextClient = 0;

    private final Stats stats = new Stats();

    public NetworkClient(Settings settings) {
        this.settings = settings.copy();
        this.nodes = SettingsUtils.discoveredOrDeclaredNodes(settings);
        // shuffle the list of nodes so in case of failures, the fallback is spread
        Collections.shuffle(nodes);

        if (SettingsUtils.hasPinnedNode(settings)) {
            // move pinned node in front to be selected (only once)
            String pinnedNode = SettingsUtils.getPinnedNode(settings);

            if (log.isDebugEnabled()) {
                log.debug("Opening (pinned) network client to " + pinnedNode);
            }

            nodes.remove(pinnedNode);
            nodes.add(0, pinnedNode);
        }

        selectNextNode();

        Assert.notNull(currentTransport, "no node information provided");
    }

    private boolean selectNextNode() {
        if (nextClient >= nodes.size()) {
            return false;
        }

        if (currentTransport != null) {
            stats.nodeRetries++;
        }

        closeTransport();
        currentNode = nodes.get(nextClient++);
        SettingsUtils.pinNode(settings, currentNode);
        currentTransport = new CommonsHttpTransport(settings, currentNode);
        return true;
    }

    public Response execute(Request request) {
        Response response = null;

        boolean newNode;
        do {
            SimpleRequest routedRequest = new SimpleRequest(request.method(), null, request.path(), request.params(), request.body());

            newNode = false;
            try {
                response = currentTransport.execute(routedRequest);
                ByteSequence body = routedRequest.body();
                if (body != null) {
                    stats.bytesSent += body.length();
                }
            } catch (Exception ex) {
                // configuration error - including SSL/PKI - bail out
                if (ex instanceof EsHadoopIllegalStateException) {
                    throw (EsHadoopException) ex;
                }
                // issues with the SSL handshake, bail out instead of retry, for security reasons
                if (ex instanceof javax.net.ssl.SSLException) {
                    throw new EsHadoopTransportException(ex);
                }
                // check for fatal, non-recoverable network exceptions

                if (ex instanceof BindException || ex instanceof NoRouteToHostException || ex instanceof UnknownHostException) {
                    throw new EsHadoopTransportException(ex);
                }

                if (log.isTraceEnabled()) {
                    log.trace(
                            String.format(
                                    "Caught exception while performing request [%s][%s] - falling back to the next node in line...",
                                    currentNode, request.path()), ex);
                }

                String failed = currentNode;

                failedNodes.put(failed, ex);

                newNode = selectNextNode();

                log.error(String.format("Node [%s] failed (%s); "
                        + (newNode ? "selected next node [" + currentNode + "]" : "no other nodes left - aborting..."),
                        failed, ex.getMessage()));

                if (!newNode) {
                    throw new EsHadoopNoNodesLeftException(failedNodes);
                }
            }
        } while (newNode);

        return response;
    }

    @Override
    public void close() {
        closeTransport();
    }

    private void closeTransport() {
        if (currentTransport != null) {
            currentTransport.close();
            stats.aggregate(currentTransport.stats());
            currentTransport = null;
        }
    }

    @Override
    public Stats stats() {
        Stats copy = new Stats(stats);
        if (currentTransport != null) {
            copy.aggregate(currentTransport.stats());
        }
        return copy;
    }

    Stats transportStats() {
        return currentTransport.stats();
    }

    public String currentNode() {
        return currentNode;
    }
}