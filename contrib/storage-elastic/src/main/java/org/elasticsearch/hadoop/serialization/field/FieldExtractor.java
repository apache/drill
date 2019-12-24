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
package org.elasticsearch.hadoop.serialization.field;

import org.elasticsearch.hadoop.serialization.SettingsAware;

/**
 * Basic extractor (for serialization purposes) of a field. Typically used with {@link SettingsAware} for configuration/injection purposes.
 *
 */
public interface FieldExtractor {

    Object NOT_FOUND = new Object();

    /**
    * Returns the associated JSON representation for the given target.
    * If the target cannot be handled, {@value #NOT_FOUND} should be returned.
    *
    * Take into account JSON formatting - either the value is raw and escaped down the stream or already returned in the appropriate format.
    * If it is returned as a String, apply escaping otherwise make sure the processor is aware of it.
    *
    * @param target
    * @return
    */
    Object field(Object target);
}
