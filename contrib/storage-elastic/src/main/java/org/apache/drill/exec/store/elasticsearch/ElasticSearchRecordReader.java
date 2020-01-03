/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.drill.exec.store.elasticsearch;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.store.AbstractRecordReader;
import org.apache.drill.exec.vector.BaseValueVector;
import org.apache.drill.exec.vector.complex.fn.JsonReader;
import org.apache.drill.exec.vector.complex.impl.VectorContainerWriter;
import org.elasticsearch.hadoop.cfg.Settings;
import org.elasticsearch.hadoop.rest.InitializationUtils;
import org.elasticsearch.hadoop.rest.RestService;
import org.elasticsearch.hadoop.rest.RestService.PartitionReader;
import org.elasticsearch.hadoop.serialization.builder.JdkValueReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class ElasticSearchRecordReader extends AbstractRecordReader {

	private static final Logger logger = LoggerFactory.getLogger(ElasticSearchRecordReader.class);
	private static final Log commonlog = LogFactory.getLog(ElasticSearchRecordReader.class);

	private final ElasticSearchStoragePlugin plugin;
	private final FragmentContext fragmentContext;
	private final boolean unionEnabled;
	private final ElasticSearchScanSpec scanSpec;
	private final Boolean enableAllTextMode;
	private final Boolean readNumbersAsDouble;
	private Set<String> fields;
	private OperatorContext operatorContext;
	private VectorContainerWriter writer;
	private Iterator<JsonNode> cursor;
	private JsonReader jsonReader;
	private OutputMutator output;
	private PartitionReader partitionReader;

	public ElasticSearchRecordReader(ElasticSearchScanSpec elasticSearchScanSpec, List<SchemaPath> columns,
			FragmentContext context, ElasticSearchStoragePlugin elasticSearchStoragePlugin) {
		// TODO
		this.fields = new HashSet<>();
		this.plugin = elasticSearchStoragePlugin;
		this.fragmentContext = context;
		this.scanSpec = elasticSearchScanSpec;
		// Fields read
		setColumns(columns);
		// TODO: What does this mean?
		this.unionEnabled = fragmentContext.getOptions().getOption(ExecConstants.ENABLE_UNION_TYPE);
		// TODO: These should be place out of Mongo attributes
		this.enableAllTextMode = fragmentContext.getOptions().getOption(ExecConstants.MONGO_ALL_TEXT_MODE).bool_val;
		this.readNumbersAsDouble = fragmentContext.getOptions().getOption(
				ExecConstants.MONGO_READER_READ_NUMBERS_AS_DOUBLE).bool_val;
	}

 

	@Override
	protected Collection<SchemaPath> transformColumns(Collection<SchemaPath> projectedColumns) {
		Set<SchemaPath> transformed = Sets.newLinkedHashSet();
		// TODO: See if we can only poll for selected columns
		if (!isStarQuery()) {
			for (SchemaPath column : projectedColumns) {
				String fieldName = column.getRootSegment().getPath();
				transformed.add(column);
				// just query for this field
				this.fields.add(fieldName);
			}
		} else {
			// Query all fields
			transformed.add(SchemaPath.STAR_COLUMN);
		}
		return transformed;
	}

	@Override
	public void setup(OperatorContext context, OutputMutator output) throws ExecutionSetupException {
		this.operatorContext = context;
		this.output = output;
		this.writer = new VectorContainerWriter(output, this.unionEnabled);

		jsonReader = new org.apache.drill.exec.vector.complex.fn.JsonReader.Builder(fragmentContext.getManagedBuffer())
			.schemaPathColumns(Lists.newArrayList(getColumns()))
			.allTextMode(enableAllTextMode)
			.readNumbersAsDouble(readNumbersAsDouble)
			.build();
	}

	@Override
	public int next() {
		if (cursor == null) {
			logger.info("Initializing cursor");
				// so in here ,it should put query fields in here.
//				this.cursor = ElasticSearchCursor.scroll(this.plugin.getClient(), this.plugin.getObjectMapper(),
//						this.scanSpec.getIndexName(), this.scanSpec.getTypeMappingName(), MapUtils.EMPTY_MAP, null);
				
				Settings settings = scanSpec.getPartitionDefinition().settings();

				InitializationUtils.setValueReaderIfNotSet(settings, JdkValueReader.class, commonlog);
				PartitionReader partitionReader = RestService.createReader(settings, scanSpec.getPartitionDefinition(), commonlog);
				try {
					cursor = (Iterator) partitionReader.scrollQuery();
				} catch (Exception e) {
					logger.debug("Error initializing cursor: {}", e.getMessage());
				}
				
			 
		}

		// Reset Data
		writer.allocate();
		writer.reset();

		int docCount = 0;
		Stopwatch watch = Stopwatch.createStarted();

		try {
			// Batch pull
			while (docCount < BaseValueVector.INITIAL_VALUE_ALLOCATION && cursor.hasNext()) {
				writer.setPosition(docCount);
				JsonNode element = cursor.next();
				// Read the data of this layer
				JsonNode id = JsonHelper.getPath(element, "_id");
				// HACK: This is done so we can poll _id from elastic into
				// object content
				ObjectNode content = (ObjectNode) JsonHelper.getPath(element, "_source");
				content.put("_id", id.asText());
				this.jsonReader.setSource(content);
				// this is using json
				this.jsonReader.write(writer);
				docCount++;
			}
			this.jsonReader.ensureAtLeastOneField(writer);
			writer.setValueCount(docCount);
			logger.debug("Took {} ms to get {} records", watch.elapsed(TimeUnit.MILLISECONDS), docCount);
			return docCount;
		} catch (IOException e) {
			String msg = "Failure while reading document. - Parser was at record: " + (docCount + 1);
			logger.error(msg, e);
			throw new DrillRuntimeException(msg, e);
		}
	}

	@Override
	public void close() throws Exception {

		if(partitionReader != null ){
			partitionReader.close();
		}
	}
}
