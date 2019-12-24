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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.elasticsearch.hadoop.cfg.Settings;
import org.elasticsearch.hadoop.rest.Resource;
import org.elasticsearch.hadoop.serialization.ParsingUtils;
import org.elasticsearch.hadoop.serialization.json.JacksonJsonParser;
import org.elasticsearch.hadoop.util.BytesArray;
import org.elasticsearch.hadoop.util.ObjectUtils;
import org.elasticsearch.hadoop.util.StringUtils;

/**
 * Dedicated extractor for field parsing. Optimized to extract all the fields in only one parsing of the document.
 */
public class JsonFieldExtractors {

    private static Log log = LogFactory.getLog(JsonFieldExtractors.class);

    private final Settings settings;

    private final List<Object> results = new ArrayList<Object>(6);
    private String[] paths;

    private FieldExtractor id, parent, routing, ttl, version, timestamp;
    private AbstractIndexExtractor indexExtractor;
    private AbstractDefaultParamsExtractor params;

    class PrecomputedFieldExtractor implements FieldExtractor {

        private final int slot;
        private final String fieldName;
        private final boolean returnAsJson;

        public PrecomputedFieldExtractor(int slot, String fieldName, boolean returnAsJson) {
            this.slot = slot;
            this.fieldName = fieldName;
            this.returnAsJson = returnAsJson;
        }

        @Override
        public Object field(Object target) {
            Object result = results.get(slot);
            if (result == ParsingUtils.NOT_FOUND) {
                return FieldExtractor.NOT_FOUND;
            }
            return (returnAsJson ? StringUtils.toJsonString(result) : result);
        }

        @Override
        public String toString() {
            return String.format("JsonExtractor for field [%s]", fieldName);
        }
    }

    private static class FixedFieldExtractor implements FieldExtractor {
        private final Object value;

        public FixedFieldExtractor(Object value) {
            this.value = value;
        }

        @Override
        public Object field(Object target) {
            return value;
        }

        @Override
        public String toString() {
            return "ConstantJsonExtractor";
        }
    }

    public JsonFieldExtractors(Settings settings) {
        this.settings = settings;

        final List<String> jsonPaths = new ArrayList<String>();

        id = init(settings.getMappingId(), jsonPaths);
        parent = init(settings.getMappingParent(), jsonPaths);
        routing = init(settings.getMappingRouting(), jsonPaths);
        ttl = init(settings.getMappingTtl(), jsonPaths);
        version = init(settings.getMappingVersion(), jsonPaths);
        timestamp = init(settings.getMappingTimestamp(), jsonPaths);

        // create index format
        indexExtractor = new AbstractIndexExtractor() {
            @Override
            protected FieldExtractor createFieldExtractor(String fieldName) {
                return createJsonFieldExtractor(fieldName, jsonPaths, false);
            }
        };
        indexExtractor.setSettings(settings);

        indexExtractor.compile(new Resource(settings, false).toString());

        // if there's no pattern, simply remove it
        indexExtractor = (indexExtractor.hasPattern() ? indexExtractor : null);

        if (settings.hasUpdateScriptParams()) {
            params = new AbstractDefaultParamsExtractor() {
                @Override
                protected FieldExtractor createFieldExtractor(String fieldName) {
                    return init(fieldName, jsonPaths);
                }
            };
            params.setSettings(settings);
        }

        paths = jsonPaths.toArray(new String[jsonPaths.size()]);
    }

    private FieldExtractor init(String fieldName, List<String> pathList) {
        if (fieldName != null) {
            Object constant = initConstant(fieldName);
            if (constant != null) {
                return new FixedFieldExtractor(constant);
            }
            else {
                return createJsonFieldExtractor(fieldName, pathList, true);
            }
        }
        return null;
    }

    private FieldExtractor createJsonFieldExtractor(String fieldName, List<String> pathList, boolean asJson) {
        pathList.add(fieldName);
        return new PrecomputedFieldExtractor(pathList.size() - 1, fieldName, asJson);
    }

    private Object initConstant(String field) {
        // don't do any escaping and pass the user JSON as is
        if (field != null && field.startsWith("<") && field.endsWith(">")) {
            return ExtractorUtils.extractConstant(field.substring(1, field.length() - 1), settings.getMappingConstantAutoQuote());
        }
        return null;
    }

    public IndexExtractor indexAndType() {
        return indexExtractor;
    }

    public FieldExtractor id() {
        return id;
    }

    public FieldExtractor parent() {
        return parent;
    }

    public FieldExtractor routing() {
        return routing;
    }

    public FieldExtractor ttl() {
        return ttl;
    }

    public FieldExtractor version() {
        return version;
    }

    public FieldExtractor timestamp() {
        return timestamp;
    }

    public void process(BytesArray storage) {
        // no extractors, no lookups
        if (ObjectUtils.isEmpty(paths)) {
            return;
        }

        results.clear();

        if (log.isTraceEnabled()) {
            log.trace(String.format("About to look for paths [%s] in doc [%s]", Arrays.toString(paths), storage));
        }

        results.addAll(ParsingUtils.values(new JacksonJsonParser(storage.bytes(), 0, storage.length()), paths));
    }

    public FieldExtractor params() {
        return params;
    }
}