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
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;

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
import com.mongodb.MongoCredential;
import com.mongodb.ReadPreference;
import com.mongodb.ServerAddress;

public class MongoRecordReader extends AbstractRecordReader {
  static final Logger logger = LoggerFactory.getLogger(MongoRecordReader.class);

  private DBCollection collection;
  private DBCursor cursor;

  private JsonReader jsonReader;
  private VectorContainerWriter writer;

  private BasicDBObject filters;
  private DBObject fields;

  private MongoClientOptions clientOptions;
  private MongoCredential credential;
  private FragmentContext fragmentContext;
  private OperatorContext operatorContext;

  private Boolean enableAllTextMode;

  public MongoRecordReader(MongoSubScan.MongoSubScanSpec subScanSpec,
      List<SchemaPath> projectedColumns, FragmentContext context,
      MongoClientOptions clientOptions, MongoCredential credential) {
    this.clientOptions = clientOptions;
    this.credential = credential;
    this.fields = new BasicDBObject();
    // exclude _id field, if not mentioned by user.
    this.fields.put(DrillMongoConstants.ID, Integer.valueOf(0));
    setColumns(projectedColumns);
    this.fragmentContext = context;
    this.filters = new BasicDBObject();
    Map<String, List<BasicDBObject>> mergedFilters = MongoUtils.mergeFilters(
        subScanSpec.getMinFilters(), subScanSpec.getMaxFilters());
    buildFilters(subScanSpec.getFilter(), mergedFilters);
    enableAllTextMode = fragmentContext.getOptions().getOption(ExecConstants.MONGO_ALL_TEXT_MODE).bool_val;
    init(subScanSpec);
  }

  @Override
  protected Collection<SchemaPath> transformColumns(
      Collection<SchemaPath> projectedColumns) {
    Set<SchemaPath> transformed = Sets.newLinkedHashSet();
    if (!isStarQuery()) {
      for (SchemaPath column : projectedColumns ) {
        String fieldName = column.getRootSegment().getPath();
        transformed.add(SchemaPath.getSimplePath(fieldName));
        this.fields.put(fieldName, Integer.valueOf(1));
      }
    } else {
      transformed.add(AbstractRecordReader.STAR_COLUMN);
    }
    return transformed;
  }

  private void buildFilters(BasicDBObject pushdownFilters,
      Map<String, List<BasicDBObject>> mergedFilters) {
    for (Entry<String, List<BasicDBObject>> entry : mergedFilters.entrySet()) {
      List<BasicDBObject> list = entry.getValue();
      if (list.size() == 1) {
        this.filters.putAll(list.get(0).toMap());
      } else {
        BasicDBObject andQueryFilter = new BasicDBObject();
        andQueryFilter.put("$and", list);
        this.filters.putAll(andQueryFilter.toMap());
      }
    }
    if (pushdownFilters != null && !pushdownFilters.toMap().isEmpty()) {
      if (!mergedFilters.isEmpty()) {
        this.filters = MongoUtils.andFilterAtIndex(this.filters,
            pushdownFilters);
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
      MongoClient client = MongoCnxnManager.getClient(addresses, clientOptions,
          credential);
      DB db = client.getDB(subScanSpec.getDbName());
      collection = db.getCollection(subScanSpec.getCollectionName());
    } catch (UnknownHostException e) {
      throw new DrillRuntimeException(e.getMessage(), e);
    }
  }

  @Override
  public void setup(OutputMutator output) throws ExecutionSetupException {
    this.writer = new VectorContainerWriter(output);
    this.jsonReader = new JsonReader(fragmentContext.getManagedBuffer(), Lists.newArrayList(getColumns()), enableAllTextMode, false);
    logger.info("Filters Applied : " + filters);
    logger.info("Fields Selected :" + fields);
    cursor = collection.find(filters, fields);
  }

  @Override
  public int next() {
    writer.allocate();
    writer.reset();

    int docCount = 0;
    Stopwatch watch = new Stopwatch();
    watch.start();

    try {
      while (docCount < BaseValueVector.INITIAL_VALUE_ALLOCATION && cursor.hasNext()) {
        writer.setPosition(docCount);
        String doc = cursor.next().toString();
        jsonReader.setSource(doc.getBytes(Charsets.UTF_8));
        jsonReader.write(writer);
        docCount++;
      }

      jsonReader.ensureAtLeastOneField(writer);

      writer.setValueCount(docCount);
      logger.debug("Took {} ms to get {} records",
          watch.elapsed(TimeUnit.MILLISECONDS), docCount);
      return docCount;
    } catch (IOException e) {
      String msg = "Failure while reading document. - Parser was at record: " + (docCount + 1);
      logger.error(msg, e);
      throw new DrillRuntimeException(msg, e);
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
