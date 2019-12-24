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
package org.elasticsearch.hadoop.rest.stats;

import org.elasticsearch.hadoop.rest.RestRepository;

/**
 * Basic class gathering stats within a {@link RestRepository} instance.
 */
public class Stats {

    /** sent */
    public long bytesSent;
    public long docsSent;
    public long docsRetried;
    public long bytesRetried;
    /** ack */
    public long bytesAccepted;
    public long docsAccepted;
    /** reads */
    public long bytesReceived;
    public long docsReceived;
    /** bulk */
    public long bulkTotal;
    public long bulkRetries;
    /** fall overs */
    public int nodeRetries;
    public int netRetries;
    /** time measured (in millis)*/
    public long netTotalTime;
    public long bulkTotalTime;
    public long bulkRetriesTotalTime;
    /** scroll */
    public long scrollTotalTime;
    public long scrollTotal;

    public Stats() {};

    public Stats(Stats stats) {
        if (stats == null) {
            return;
        }

        this.bytesSent = stats.bytesSent;
        this.docsSent = stats.docsSent;
        this.bulkTotal = stats.bulkTotal;

        this.docsRetried = stats.docsRetried;
        this.bytesRetried = stats.bytesRetried;
        this.bulkRetries = stats.bulkRetries;

        this.bytesAccepted = stats.bytesAccepted;
        this.docsAccepted = stats.docsAccepted;

        this.bytesReceived = stats.bytesReceived;
        this.docsReceived = stats.docsReceived;

        this.nodeRetries = stats.nodeRetries;
        this.netRetries = stats.netRetries;

        this.netTotalTime = stats.netTotalTime;
        this.bulkTotalTime = stats.bulkTotalTime;
        this.bulkRetriesTotalTime = stats.bulkRetriesTotalTime;

        this.scrollTotal = stats.scrollTotal;
        this.scrollTotalTime = stats.scrollTotalTime;
    }

    public Stats aggregate(Stats other) {
        if (other == null) {
            return this;
        }

        bytesSent += other.bytesSent;
        docsSent += other.docsSent;
        bulkTotal += other.bulkTotal;
        docsRetried += other.docsRetried;
        bytesRetried += other.bytesRetried;
        bulkRetries += other.bulkRetries;
        bytesAccepted += other.bytesAccepted;
        docsAccepted += other.docsAccepted;

        bytesReceived += other.bytesReceived;
        docsReceived += other.docsReceived;

        nodeRetries += other.nodeRetries;
        netRetries += other.netRetries;

        netTotalTime += other.netTotalTime;
        bulkTotalTime += other.bulkTotalTime;
        bulkRetriesTotalTime += other.bulkRetriesTotalTime;

        scrollTotal += other.scrollTotal;
        scrollTotalTime += other.scrollTotalTime;

        return this;
    }
}