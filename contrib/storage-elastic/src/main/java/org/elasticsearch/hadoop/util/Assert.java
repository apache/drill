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
package org.elasticsearch.hadoop.util;

import org.elasticsearch.hadoop.EsHadoopIllegalArgumentException;

/**
 * Assertion utility used for validating arguments.
 */
public abstract class Assert {

    public static void hasText(CharSequence sequence, String message) {
        if (!StringUtils.hasText(sequence)) {
            throw new EsHadoopIllegalArgumentException(message);
        }
    }

    public static void hasText(CharSequence sequence) {
        hasText(sequence, "[Assertion failed] - this CharSequence argument must have text; it must not be null, empty, or blank");
    }

    public static void notNull(Object object, String message) {
        if (object == null) {
            throw new EsHadoopIllegalArgumentException(message);
        }
    }

    public static void notNull(Object object) {
        notNull(object, "[Assertion failed] - this argument is required; it must not be null");
    }

    public static void isTrue(Boolean object, String message) {
        if (!Boolean.TRUE.equals(object)) {
            throw new EsHadoopIllegalArgumentException(message);
        }
    }

    public static void isTrue(Boolean object) {
        isTrue(object, "[Assertion failed] - this argument must be true");
    }
}
