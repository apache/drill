/**
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
package org.apache.drill.exec.store.mongo;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.PathSegment;
import org.apache.drill.common.expression.PathSegment.NameSegment;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.store.AbstractRecordReader;
import org.apache.drill.exec.vector.NullableVarCharVector;
import org.apache.drill.exec.vector.VarCharVector;
import org.apache.drill.exec.vector.complex.fn.JsonReaderWithState;
import org.apache.drill.exec.vector.complex.impl.VectorContainerWriter;
import org.apache.drill.exec.vector.complex.writer.BaseWriter;
import org.apache.drill.exec.vector.complex.writer.BaseWriter.MapWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.ReadPreference;
import com.mongodb.ServerAddress;

public class MongoRecordReader extends AbstractRecordReader {
  static final Logger logger = LoggerFactory.getLogger(MongoRecordReader.class);

  private static final int TARGET_RECORD_COUNT = 3000;

  private DBCollection collection;
  private DBCursor cursor;

  private OutputMutator outputMutator;
  private NullableVarCharVector valueVector;

  private JsonReaderWithState jsonReaderWithState;
  private VectorContainerWriter writer;
  private List<SchemaPath> columns;

  private BasicDBObject filters;
  private DBObject fields;

  private MongoClientOptions clientOptions;
  private FragmentContext fragmentContext;
  private OperatorContext operatorContext;
  
  private Boolean enableAllTextMode;
  
  private boolean firstTime = true;

  public MongoRecordReader(MongoSubScan.MongoSubScanSpec subScanSpec, List<SchemaPath> projectedColumns,
      FragmentContext context, MongoClientOptions clientOptions) {
    this.clientOptions = clientOptions;
    this.fields = new BasicDBObject();
    // exclude _id field, if not mentioned by user.
    this.fields.put(DrillMongoConstants.ID, Integer.valueOf(0));
    this.columns = projectedColumns;
    setColumns(projectedColumns);
    transformColumns(projectedColumns);
    this.fragmentContext = context;
    this.filters = new BasicDBObject();
    Map<String, List<BasicDBObject>> mergedFilters = MongoUtils.mergeFilters(
        subScanSpec.getMinFilters(), subScanSpec.getMaxFilters());
    buildFilters(subScanSpec.getFilter(), mergedFilters);
    enableAllTextMode = fragmentContext.getDrillbitContext().getOptionManager().getOption(ExecConstants.MONGO_ALL_TEXT_MODE).bool_val;
    init(subScanSpec);
  }


	@Override
	protected Collection<SchemaPath> transformColumns(
			Collection<SchemaPath> projectedColumns) {
		Set<SchemaPath> transformed = Sets.newLinkedHashSet();
		if (!isStarQuery()) {
			Iterator<SchemaPath> columnIterator = projectedColumns.iterator();
			while (columnIterator.hasNext()) {
				SchemaPath column = columnIterator.next();
				NameSegment root = column.getRootSegment();
				String fieldName = root.getPath();
				transformed.add(SchemaPath.getSimplePath(fieldName));
				this.fields.put(fieldName, Integer.valueOf(1));
			}
		}
		return transformed;
	}

  private void buildFilters(BasicDBObject pushdownFilters, Map<String, List<BasicDBObject>> mergedFilters) {
    for(Entry<String, List<BasicDBObject>> entry : mergedFilters.entrySet()) {
      List<BasicDBObject> list = entry.getValue();
      if(list.size() == 1) {
        this.filters.putAll(list.get(0).toMap());
      } else {
        BasicDBObject andQueryFilter = new BasicDBObject();
        andQueryFilter.put("$and", list);
        this.filters.putAll(andQueryFilter.toMap());
      }
    }
    if (pushdownFilters != null && !pushdownFilters.toMap().isEmpty()) {
      if (!mergedFilters.isEmpty()) {
        this.filters = MongoUtils.andFilterAtIndex(this.filters, pushdownFilters);
      } else {
        this.filters = pushdownFilters;
      }
    }
  }

  private void init(MongoSubScan.MongoSubScanSpec subScanSpec) {
    try {
      List<String> hosts = subScanSpec.getHosts();
      List<ServerAddress> addresses = Lists.newArrayList();
      for (String host : hosts) {
        addresses.add(new ServerAddress(host));
      }
      MongoClient client = MongoCnxnManager.getClient(addresses, clientOptions);
      DB db = client.getDB(subScanSpec.getDbName());
      db.setReadPreference(ReadPreference.nearest());
      collection = db.getCollection(subScanSpec.getCollectionName());
    } catch (UnknownHostException e) {
      throw new DrillRuntimeException(e.getMessage(), e);
    }
  }
  
  @Override
  public void setup(OutputMutator output) throws ExecutionSetupException {
    try {
      this.writer = new VectorContainerWriter(output);
      this.jsonReaderWithState = new JsonReaderWithState(fragmentContext.getManagedBuffer(), columns, enableAllTextMode);
    } catch (IOException e) {
      throw new ExecutionSetupException("Failure in Mongo JsonReader initialization.", e);
    }
    this.outputMutator = output;
    logger.info("Filters Applied : " + filters);
    logger.info("Fields Selected :" + fields);
    cursor = collection.find(filters, fields);
    if (isStarQuery()) {
      try {
      SchemaPath startColumn = SchemaPath.getSimplePath("*");
      MaterializedField field = MaterializedField.create(startColumn,
          Types.optional(MinorType.VARCHAR));
        valueVector = outputMutator
            .addField(field, NullableVarCharVector.class);
      } catch (SchemaChangeException e) {
        throw new ExecutionSetupException(e);
      }
    }
  }
  
  private boolean handleStarQueryWithEmptyResults() {
    return fields.toMap().size() == 1 && fields.containsField(DrillMongoConstants.ID);
  }

  @Override
  public int next() {
    writer.allocate();
    writer.reset();

    int docCount = 0;
    Stopwatch watch = new Stopwatch();
    watch.start();
    int rowCount = 0;
    
    if (firstTime && !cursor.hasNext() && handleStarQueryWithEmptyResults()) {
      firstTime = false;
      MapWriter mapVector = writer.rootAsMap();
      mapVector.varChar("*");
      writer.setValueCount(0);
      return 0;
    }
     firstTime = false;

    if (isStarQuery()) {
      if (valueVector != null) {
        valueVector.clear();
        valueVector.allocateNew();
      }
    }
    try {
      done: for (; rowCount < TARGET_RECORD_COUNT && cursor.hasNext(); rowCount++) {
        writer.setPosition(docCount);
        DBObject record = cursor.next();
        if (isStarQuery()) {
          if (valueVector != null) {
            byte[] bytes = record.toString().getBytes();
            if (!valueVector.getMutator().setSafe(rowCount, bytes, 0,
                bytes.length)) {
              break done;
            }
            docCount++;
          } else {
            logger.debug("valueVector is null");
            break done;
          }
          continue;
        } else {
          switch (jsonReaderWithState.write(
              record.toString().getBytes(Charsets.UTF_8), writer)) {
          case WRITE_SUCCEED:
            docCount++;
            break;

          case WRITE_FAILED:
            if (docCount == 0) {
              throw new DrillRuntimeException(
                  "Record is too big to fit into allocated ValueVector");
            }
            logger.warn(
                "Record {} is too big to fit into allocated ValueVector",
                record);
            break done;

          default:
            break done;
          }
        }
      }
      
      for (SchemaPath sp :jsonReaderWithState.getNullColumns() ) {
        PathSegment root = sp.getRootSegment();
        BaseWriter.MapWriter fieldWriter = writer.rootAsMap();
        if (root.getChild() != null && ! root.getChild().isArray()) {
          fieldWriter = fieldWriter.map(root.getNameSegment().getPath());
          while ( root.getChild().getChild() != null && ! root.getChild().isArray() ) {
            fieldWriter = fieldWriter.map(root.getChild().getNameSegment().getPath());
            root = root.getChild();
          }
          fieldWriter.integer(root.getChild().getNameSegment().getPath());
        } else  {
          fieldWriter.integer(root.getNameSegment().getPath());
        }
      }
      
      writer.setValueCount(docCount);
      setOutputDocCount(docCount);
      logger.debug("Took {} ms to get {} records", watch.elapsed(TimeUnit.MILLISECONDS), rowCount);
      return docCount;
    } catch (Exception e) {
      logger.error(e.getMessage(), e);
      throw new DrillRuntimeException("Failure while reading Mongo Record.", e);
    }
  }

  private void setOutputDocCount(int docCount) {
    if (isStarQuery() && valueVector != null) {
      valueVector.getMutator().setValueCount(docCount);
    }
  }

  @Override
  public void cleanup() {
  }

  public OperatorContext getOperatorContext() {
    return operatorContext;
  }

  @Override
  public void setOperatorContext(OperatorContext operatorContext) {
    this.operatorContext = operatorContext;
  }

}
