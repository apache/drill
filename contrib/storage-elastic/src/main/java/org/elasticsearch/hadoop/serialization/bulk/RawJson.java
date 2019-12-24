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
package org.elasticsearch.hadoop.serialization.bulk;

import org.elasticsearch.hadoop.util.StringUtils;

// wrapper class around values already converted to JSON
// used currently when dealing with script parameters which cause a FieldExtractor to actually create a String template
// which might contain raw json (constants or JSON extracts) or raw objects (when doing extraction)
public class RawJson {
    private final String source;

    public RawJson(String source) {
        this.source = source;
    }

    public byte[] json() {
        return StringUtils.toUTF(source);
    }

    @Override
    public String toString() {
        return source;
    }

    @Override
    public int hashCode() {
        return (source == null ? 0 : source.hashCode());
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        RawJson other = (RawJson) obj;
        if (source == null) {
            if (other.source != null)
                return false;
        }
        else if (!source.equals(other.source))
            return false;
        return true;
    }
}
