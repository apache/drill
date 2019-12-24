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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.elasticsearch.hadoop.cfg.Settings;
import org.elasticsearch.hadoop.serialization.SettingsAware;
import org.elasticsearch.hadoop.util.unit.TimeValue;

public class SimpleHttpRetryPolicy implements HttpRetryPolicy, SettingsAware {

    private static Log log = LogFactory.getLog(SimpleHttpRetryPolicy.class);

    private int retryLimit;
    private long retryTime;

    private class SimpleRetry implements Retry {
        private int retryCount = 0;

        @Override
        public boolean retry(int httpStatus) {
            // everything fine, no need to retry
            if (HttpStatus.isSuccess(httpStatus)) {
                return false;
            }

            switch (httpStatus) {
            // ES is busy, allow retries
            case HttpStatus.TOO_MANY_REQUESTS:
            case HttpStatus.SERVICE_UNAVAILABLE:
                if (retryLimit < 0 || ++retryCount < retryLimit) {
                    try {
                        if (log.isDebugEnabled()) {
                            log.debug(String.format("Elasticsearch service unavailable - retrying in %s",
                                    TimeValue.timeValueMillis((retryTime))));
                        }
                        Thread.sleep(retryTime);
                        return true;
                    } catch (InterruptedException e) {
                        if (log.isDebugEnabled()) {
                            log.debug(String.format("Thread interrupted - giving up on retrying..."));
                        }

                        return false;
                    }
                }
                return false;
            default:
                return false;
            }
        }
    }

    @Override
    public Retry init() {
        return new SimpleRetry();
    }

    @Override
    public void setSettings(Settings settings) {
        retryLimit = settings.getBatchWriteRetryCount();
        retryTime = settings.getBatchWriteRetryWait();
    }
}