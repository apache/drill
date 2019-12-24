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
package org.elasticsearch.hadoop.serialization;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.exec.store.elasticsearch.JsonHelper;
import org.elasticsearch.hadoop.cfg.Settings;
import org.elasticsearch.hadoop.serialization.builder.ValueReader;
import org.elasticsearch.hadoop.serialization.dto.mapping.Field;
import org.elasticsearch.hadoop.util.Assert;
import org.elasticsearch.hadoop.util.StringUtils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Class handling the conversion of data from ES to target objects. It performs tree navigation tied to a potential ES mapping (if available).
 * Expected to read a _search response.
 */
public class ScrollReader {
	   private static final String SCROLL = "scroll";
	  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();


	  public static class Scroll {
	        private final String scrollId;
	        private   Iterator<JsonNode> iterator;
	        private long totalHits;
	        
	        protected Scroll(String scrollId, Iterator<JsonNode> iterator ,long totalHits) {
	            this.scrollId = scrollId;
	            this.iterator = iterator;
	            this.totalHits = totalHits;
	        }
	        

	        public String getScrollId() {
	            return scrollId;
	        }


			public long getTotalHits() {
				return totalHits;
			}

			public void setTotalHits(long totalHits) {
				this.totalHits = totalHits;
			}


			public Iterator<JsonNode> getIterator() {
				return iterator;
			}


			public void setIterator(Iterator<JsonNode> iterator) {
				this.iterator = iterator;
			}
	        
	        
	        
	    }


    public static class ScrollReaderConfig {
        public ValueReader reader;

        public boolean readMetadata;
        public String metadataName;
        public boolean returnRawJson;
        public boolean ignoreUnmappedFields;
        public List<String> includeFields;
        public List<String> excludeFields;
        public Field rootField;

        public ScrollReaderConfig(ValueReader reader, Field rootField, boolean readMetadata, String metadataName,
                boolean returnRawJson, boolean ignoreUnmappedFields, List<String> includeFields,
                List<String> excludeFields) {
            super();
            this.reader = reader;
            this.readMetadata = readMetadata;
            this.metadataName = metadataName;
            this.returnRawJson = returnRawJson;
            this.ignoreUnmappedFields = ignoreUnmappedFields;
            this.includeFields = includeFields;
            this.excludeFields = excludeFields;
            this.rootField = rootField;
        }

        public ScrollReaderConfig(ValueReader reader, Field rootField, boolean readMetadata, String metadataName, boolean returnRawJson, boolean ignoreUnmappedFields) {
            this(reader, rootField, readMetadata, metadataName, returnRawJson, ignoreUnmappedFields, Collections.<String> emptyList(), Collections.<String> emptyList());
        }

        public ScrollReaderConfig(ValueReader reader) {
            this(reader, null, false, "_metadata", false, false, Collections.<String> emptyList(), Collections.<String> emptyList());
        }

        public ScrollReaderConfig(ValueReader reader, Field field, Settings cfg) {
            this(reader, field, cfg.getReadMetadata(), cfg.getReadMetadataField(),
                    cfg.getOutputAsJson(), cfg.getReadMappingMissingFieldsIgnore(),
                    StringUtils.tokenize(cfg.getReadFieldInclude()), StringUtils.tokenize(cfg.getReadFieldExclude()));
        }
    }

    private static final Log log = LogFactory.getLog(ScrollReader.class);

    private final Map<String, FieldType> esMapping;
    private final boolean readMetadata;
    private final String metadataField;

    private ObjectMapper objMapper = null;
    
    
    public ScrollReader(ScrollReaderConfig scrollConfig) {
        this.objMapper = OBJECT_MAPPER;
        this.readMetadata = scrollConfig.readMetadata;
        this.metadataField = scrollConfig.metadataName;
        Field mapping = scrollConfig.rootField;
        
        this.esMapping = Field.toLookupMap(mapping);
    }

    public Scroll read(InputStream content) throws IOException {
        Assert.notNull(content);
        
        JsonNode rootNode = JsonHelper.readRespondeContentAsJsonTree(objMapper, content);
        // 遍历id
        JsonNode scrollIdNode = JsonHelper.getPath(rootNode, "_scroll_id");
        String scrollId;
        if (!scrollIdNode.isMissingNode()) {
            scrollId = scrollIdNode.asText();
        } else {
            throw new DrillRuntimeException("Couldn't get '"+SCROLL+"' for cursor");
        }
        // 命中个数
        JsonNode totalHitsNode = JsonHelper.getPath(rootNode, "hits.total");
        long totalHits = 0;
        if (!totalHitsNode.isMissingNode()) {
            totalHits = totalHitsNode.asLong();
        } else {
            throw new DrillRuntimeException("Couldn't get 'hits.total' for cursor");
        }

        //结果数据
        JsonNode elementsNode = JsonHelper.getPath(rootNode, "hits.hits");
        Iterator<JsonNode> elementIterator;
        if (!elementsNode.isMissingNode() && elementsNode.isArray()) {
            elementIterator = elementsNode.iterator();
        } else {
            throw new DrillRuntimeException("Couldn't get 'hits.hits' for cursor");
        }
        return new Scroll(scrollId, elementIterator ,totalHits);
    }

}