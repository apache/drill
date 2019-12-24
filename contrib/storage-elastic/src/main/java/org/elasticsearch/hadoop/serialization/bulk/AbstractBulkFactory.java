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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.elasticsearch.hadoop.EsHadoopIllegalArgumentException;
import org.elasticsearch.hadoop.cfg.Settings;
import org.elasticsearch.hadoop.rest.Resource;
import org.elasticsearch.hadoop.serialization.builder.ValueWriter;
import org.elasticsearch.hadoop.serialization.bulk.MetadataExtractor.Metadata;
import org.elasticsearch.hadoop.serialization.field.ConstantFieldExtractor;
import org.elasticsearch.hadoop.serialization.field.FieldExplainer;
import org.elasticsearch.hadoop.serialization.field.FieldExtractor;
import org.elasticsearch.hadoop.serialization.field.IndexExtractor;
import org.elasticsearch.hadoop.serialization.field.JsonFieldExtractors;
import org.elasticsearch.hadoop.serialization.json.JacksonJsonGenerator;
import org.elasticsearch.hadoop.util.BytesArray;
import org.elasticsearch.hadoop.util.BytesArrayPool;
import org.elasticsearch.hadoop.util.FastByteArrayOutputStream;
import org.elasticsearch.hadoop.util.ObjectUtils;
import org.elasticsearch.hadoop.util.StringUtils;

public abstract class AbstractBulkFactory implements BulkFactory {

    private static Log log = LogFactory.getLog(AbstractBulkFactory.class);

    protected Settings settings;

    private final boolean jsonInput;
    private final boolean isStatic;

    private final MetadataExtractor metaExtractor;
    // used when specifying an index pattern
    private IndexExtractor indexExtractor;
    private FieldExtractor idExtractor,
    typeExtractor,
    parentExtractor,
    routingExtractor,
    versionExtractor,
    ttlExtractor,
    timestampExtractor,
    paramsExtractor;

    private final FieldExtractor versionTypeExtractor = new FieldExtractor() {

        private Object value;

        @Override
        public Object field(Object target) {
            // lazy init to have the settings in place
            if (value == null) {
                value = new RawJson(StringUtils.toJsonString(settings.getMappingVersionType()));
            }
            return value;
        }
    };

    private JsonFieldExtractors jsonExtractors;

    private final ValueWriter valueWriter;

    class FieldWriter {
        final FieldExtractor extractor;
        final BytesArrayPool pool = new BytesArrayPool();

        FieldWriter(FieldExtractor extractor) {
            this.extractor = extractor;
        }

        BytesArrayPool write(Object object) {
            pool.reset();

            Object value = extractor.field(object);
            if (value == FieldExtractor.NOT_FOUND) {
                String obj = (extractor instanceof FieldExplainer ? ((FieldExplainer) extractor).toString(object) : object.toString());
                throw new EsHadoopIllegalArgumentException(String.format("[%s] cannot extract value from entity [%s] | instance [%s]", extractor, obj.getClass(), obj));
            }

            if (value instanceof List) {
                for (Object val : (List) value) {
                    doWrite(val);
                }
            }
            // if/else to save one collection/iterator instance
            else {
                doWrite(value);
            }

            return pool;
        }

        void doWrite(Object value) {
            // common-case - constants or JDK types
            if (value instanceof String || jsonInput || value instanceof Number || value instanceof Boolean || value == null) {
                String valueString = (value == null ? "null" : value.toString());
                if (value instanceof String && !jsonInput) {
                    valueString = StringUtils.toJsonString(valueString);
                }

                pool.get().bytes(valueString);
            }
            else if (value instanceof RawJson) {
                pool.get().bytes(((RawJson) value).json());
            }
            // library specific type - use the value writer (a bit overkill but handles collections/arrays properly)
            else {
                BytesArray ba = pool.get();
                JacksonJsonGenerator generator = new JacksonJsonGenerator(new FastByteArrayOutputStream(ba));
                valueWriter.write(value, generator);
                generator.flush();
                generator.close();
            }
        }

        @Override
        public String toString() {
            return "FieldWriter for " + extractor;
        }
    }

    interface DynamicContentRef {
        List<Object> getDynamicContent();
    }

    public class DynamicHeaderRef implements DynamicContentRef {
        final List<Object> header = new ArrayList<Object>();

        public List<Object> getDynamicContent() {
            header.clear();
            writeObjectHeader(header);
            return compact(header);
        }
    }

    public class DynamicEndRef implements DynamicContentRef {
        final List<Object> end = new ArrayList<Object>();

        public List<Object> getDynamicContent() {
            end.clear();
            writeObjectEnd(end);
            return compact(end);
        }
    }


    AbstractBulkFactory(Settings settings, MetadataExtractor metaExtractor) {
        this.settings = settings;
        this.valueWriter = ObjectUtils.instantiate(settings.getSerializerValueWriterClassName(), settings);
        this.metaExtractor = metaExtractor;

        jsonInput = settings.getInputAsJson();
        isStatic = metaExtractor == null;
        initExtractorsFromSettings(settings);
    }

    private void initExtractorsFromSettings(final Settings settings) {
        if (jsonInput) {
            if (log.isDebugEnabled()) {
                log.debug("JSON input; using internal field extractor for efficient parsing...");
            }

            jsonExtractors = new JsonFieldExtractors(settings);
            indexExtractor = jsonExtractors.indexAndType();

            idExtractor = jsonExtractors.id();
            parentExtractor = jsonExtractors.parent();
            routingExtractor = jsonExtractors.routing();
            versionExtractor = jsonExtractors.version();
            ttlExtractor = jsonExtractors.ttl();
            timestampExtractor = jsonExtractors.timestamp();
            paramsExtractor = jsonExtractors.params();
        }
        else {
            // init extractors (if needed)
            if (settings.getMappingId() != null) {
                settings.setProperty(ConstantFieldExtractor.PROPERTY, settings.getMappingId());
                idExtractor = ObjectUtils.<FieldExtractor> instantiate(settings.getMappingIdExtractorClassName(),
                        settings);
            }
            if (settings.getMappingParent() != null) {
                settings.setProperty(ConstantFieldExtractor.PROPERTY, settings.getMappingParent());
                parentExtractor = ObjectUtils.<FieldExtractor> instantiate(
                        settings.getMappingParentExtractorClassName(), settings);
            }
            if (settings.getMappingRouting() != null) {
                settings.setProperty(ConstantFieldExtractor.PROPERTY, settings.getMappingRouting());
                routingExtractor = ObjectUtils.<FieldExtractor> instantiate(
                        settings.getMappingRoutingExtractorClassName(), settings);
            }
            if (settings.getMappingTtl() != null) {
                settings.setProperty(ConstantFieldExtractor.PROPERTY, settings.getMappingTtl());
                ttlExtractor = ObjectUtils.<FieldExtractor> instantiate(settings.getMappingTtlExtractorClassName(),
                        settings);
            }
            if (settings.getMappingVersion() != null) {
                settings.setProperty(ConstantFieldExtractor.PROPERTY, settings.getMappingVersion());
                versionExtractor = ObjectUtils.<FieldExtractor> instantiate(
                        settings.getMappingVersionExtractorClassName(), settings);
            }
            if (settings.getMappingTimestamp() != null) {
                settings.setProperty(ConstantFieldExtractor.PROPERTY, settings.getMappingTimestamp());
                timestampExtractor = ObjectUtils.<FieldExtractor> instantiate(
                        settings.getMappingTimestampExtractorClassName(), settings);
            }

            // create adapter
            IndexExtractor iformat = ObjectUtils.<IndexExtractor> instantiate(settings.getMappingIndexExtractorClassName(), settings);
            iformat.compile(new Resource(settings, false).toString());

            if (iformat.hasPattern()) {
                indexExtractor = iformat;
            }

            if (settings.hasUpdateScriptParams()) {
                settings.setProperty(ConstantFieldExtractor.PROPERTY, settings.getUpdateScriptParams());
                paramsExtractor = ObjectUtils.instantiate(settings.getMappingParamsExtractorClassName(), settings);
            }

            if (log.isTraceEnabled()) {
                log.trace(String.format("Instantiated value writer [%s]", valueWriter));
                if (idExtractor != null) {
                    log.trace(String.format("Instantiated id extractor [%s]", idExtractor));
                }
                if (parentExtractor != null) {
                    log.trace(String.format("Instantiated parent extractor [%s]", parentExtractor));
                }
                if (routingExtractor != null) {
                    log.trace(String.format("Instantiated routing extractor [%s]", routingExtractor));
                }
                if (ttlExtractor != null) {
                    log.trace(String.format("Instantiated ttl extractor [%s]", ttlExtractor));
                }
                if (versionExtractor != null) {
                    log.trace(String.format("Instantiated version extractor [%s]", versionExtractor));
                }
                if (timestampExtractor != null) {
                    log.trace(String.format("Instantiated timestamp extractor [%s]", timestampExtractor));
                }
                if (paramsExtractor != null) {
                    log.trace(String.format("Instantiated params extractor [%s]", paramsExtractor));
                }
            }
        }

        // json params override other extractors
        if (settings.hasUpdateScriptParamsJson()) {
            paramsExtractor = new FieldExtractor() {
                @Override
                public Object field(Object target) {
                    return new RawJson(settings.getUpdateScriptParamsJson().trim());
                }
            };
        }
    }


    class DynamicFieldExtractor implements FieldExtractor {

        private final List<Object> before = new ArrayList<Object>();

        @Override
        public Object field(Object target) {
            before.clear();
            writeObjectHeader(before);
            return compact(before);
        }
    }

    @Override
    public BulkCommand createBulk() {
        List<Object> before = new ArrayList<Object>();
        List<Object> after = new ArrayList<Object>();

        if (!isStatic) {
            before.add(new DynamicHeaderRef());
            after.add(new DynamicEndRef());
        }
        else {
            writeObjectHeader(before);
            before = compact(before);
            writeObjectEnd(after);
            after = compact(after);
        }

        boolean isScriptUpdate = settings.hasUpdateScript();
        // compress pieces
        if (jsonInput) {
            if (isScriptUpdate) {
                return new JsonScriptTemplateBulk(before, after, jsonExtractors, settings);
            }
            return new JsonTemplatedBulk(before, after, jsonExtractors, settings);
        }
        if (isScriptUpdate) {
            return new ScriptTemplateBulk(settings, before, after, valueWriter);
        }
        return new TemplatedBulk(before, after, valueWriter);
    }

    // write action & metadata header
    protected void writeObjectHeader(List<Object> list) {
        // action
        list.add("{\"" + getOperation() + "\":{");

        // flag indicating whether a comma needs to be added between fields
        boolean commaMightBeNeeded = false;

        commaMightBeNeeded = addExtractorOrDynamicValue(list, getExtractorOrDynamicValue(Metadata.INDEX, indexExtractor), "", commaMightBeNeeded);
        commaMightBeNeeded = addExtractorOrDynamicValue(list, getExtractorOrDynamicValue(Metadata.TYPE, typeExtractor), "\"_type\":", commaMightBeNeeded);
        commaMightBeNeeded = id(list, commaMightBeNeeded);
        commaMightBeNeeded = addExtractorOrDynamicValue(list, getExtractorOrDynamicValue(Metadata.PARENT, parentExtractor), "\"_parent\":", commaMightBeNeeded);
        commaMightBeNeeded = addExtractorOrDynamicValue(list, getExtractorOrDynamicValue(Metadata.ROUTING, routingExtractor), "\"_routing\":", commaMightBeNeeded);
        commaMightBeNeeded = addExtractorOrDynamicValue(list, getExtractorOrDynamicValue(Metadata.TTL, ttlExtractor), "\"_ttl\":", commaMightBeNeeded);
        commaMightBeNeeded = addExtractorOrDynamicValue(list, getExtractorOrDynamicValue(Metadata.TIMESTAMP, timestampExtractor), "\"_timestamp\":", commaMightBeNeeded);

        // version & version_type fields
        Object versionField = getExtractorOrDynamicValue(Metadata.VERSION, versionExtractor);
        if (versionField != null) {
            if (commaMightBeNeeded) {
                list.add(",");
                commaMightBeNeeded = false;
            }
            commaMightBeNeeded = true;
            list.add("\"_version\":");
            list.add(versionField);

            // version_type - only needed when a version is specified
            Object versionTypeField = getExtractorOrDynamicValue(Metadata.VERSION_TYPE, versionTypeExtractor);
            if (versionTypeField != null) {
                if (commaMightBeNeeded) {
                    list.add(",");
                    commaMightBeNeeded = false;
                }
                commaMightBeNeeded = true;
                list.add("\"_version_type\":");
                list.add(versionTypeField);
            }
        }

        // useful for update command
        otherHeader(list, commaMightBeNeeded);
        list.add("}}\n");
    }

    protected boolean id(List<Object> list, boolean commaMightBeNeeded) {
        return addExtractorOrDynamicValue(list, getExtractorOrDynamicValue(Metadata.ID, idExtractor), "\"_id\":", commaMightBeNeeded);
    }

    // trivial utility that adds a comma before the current field alongside but only if the extractor is present
    private boolean addExtractorOrDynamicValue(List<Object> list, Object extractor, String header, boolean commaMightBeNeeded) {
        if (extractor != null) {
            if (commaMightBeNeeded) {
                list.add(",");
            }
            list.add(header);
            list.add(extractor);
            return true;
        }
        return commaMightBeNeeded;
    }

    protected void otherHeader(List<Object> list, boolean commaMightBeNeeded) {
        // no-op
    }

    // get the extractor for a given field, trying first the dynamic one, with a fallback on the 'static' one
    protected Object getExtractorOrDynamicValue(Metadata meta, FieldExtractor fallbackExtractor) {
        if (metaExtractor != null) {
            FieldExtractor metaFE = metaExtractor.get(meta);
            if (metaFE != null) {
                return metaFE;
            }
        }
        return fallbackExtractor;
    }

    protected abstract String getOperation();

    protected void writeObjectEnd(List<Object> list) {
        list.add("\n");
    }

    // optimization method used when dealing with 'static' extractors
    // concatenates all the strings to minimize the amount of data needed for construction
    private List<Object> compact(List<Object> list) {
        if (list == null || list.isEmpty()) {
            return null;
        }

        List<Object> compacted = new ArrayList<Object>();
        StringBuilder stringAccumulator = new StringBuilder();
        for (Object object : list) {
            if (object instanceof FieldExtractor) {
                if (stringAccumulator.length() > 0) {
                    compacted.add(new BytesArray(stringAccumulator.toString()));
                    stringAccumulator.setLength(0);
                }
                compacted.add(new FieldWriter((FieldExtractor) object));
            }
            else {
                stringAccumulator.append(object.toString());
            }
        }

        if (stringAccumulator.length() > 0) {
            compacted.add(new BytesArray(stringAccumulator.toString()));
        }
        return compacted;
    }

    protected FieldExtractor getParamExtractor() {
        return paramsExtractor;
    }
}