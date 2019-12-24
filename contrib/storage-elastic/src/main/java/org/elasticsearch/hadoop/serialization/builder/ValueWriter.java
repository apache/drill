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
package org.elasticsearch.hadoop.serialization.builder;

import org.elasticsearch.hadoop.serialization.Generator;

/**
 * Translates a value to its JSON-like structure.

 * Implementations should handle filtering of field names.
 */
public interface ValueWriter<T> {

    public final class Result {
        private static final Result SUCCESFUL = new Result(null);

        final Object unknownValue;

        private Result(Object target) {
            unknownValue = target;
        }

        public boolean isSuccesful() {
            return SUCCESFUL == this;
        }

        public static Result SUCCESFUL() {
            return SUCCESFUL;
        }

        public static Result FAILED(Object target) {
            return new Result(target);
        }
    }

    /**
     * Returns true if the value was written, false otherwise.
     *
     * @param object
     * @param generator
     * @return {@link Result#SUCCESFUL()} if completed, {@link Result#FAILED(Object)} otherwise containing the failing target
     */
    Result write(T object, Generator generator);
}
