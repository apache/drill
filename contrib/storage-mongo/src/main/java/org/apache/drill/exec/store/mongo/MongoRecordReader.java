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
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;

public class MongoRecordReader extends AbstractRecordReader {
  private static final Logger logger = LoggerFactory.getLogger(MongoRecordReader.class);

  private MongoCollection<Document> collection;
  private MongoCursor<Document> cursor;

  private JsonReader jsonReader;
  private VectorContainerWriter writer;

  private BasicDBObject filters;
  private final BasicDBObject fields;

  private final FragmentContext fragmentContext;
  private OperatorContext operatorContext;

  private final MongoStoragePlugin plugin;

  private final boolean enableAllTextMode;
  private final boolean readNumbersAsDouble;

  public MongoRecordReader(
      MongoSubScan.MongoSubScanSpec subScanSpec,
      List<SchemaPath> projectedColumns,
      FragmentContext context,
      MongoStoragePlugin plugin) {

    fields = new BasicDBObject();
    // exclude _id field, if not mentioned by user.
    fields.put(DrillMongoConstants.ID, Integer.valueOf(0));
    setColumns(projectedColumns);
    fragmentContext = context;
    this.plugin = plugin;
    filters = new BasicDBObject();
    Map<String, List<BasicDBObject>> mergedFilters = MongoUtils.mergeFilters(
        subScanSpec.getMinFilters(), subScanSpec.getMaxFilters());
    buildFilters(subScanSpec.getFilter(), mergedFilters);
    enableAllTextMode = fragmentContext.getOptions().getOption(ExecConstants.MONGO_ALL_TEXT_MODE).bool_val;
    readNumbersAsDouble = fragmentContext.getOptions().getOption(ExecConstants.MONGO_READER_READ_NUMBERS_AS_DOUBLE).bool_val;
    init(subScanSpec);
  }

  @Override
  protected Collection<SchemaPath> transformColumns(Collection<SchemaPath> projectedColumns) {
    Set<SchemaPath> transformed = Sets.newLinkedHashSet();
    if (!isStarQuery()) {
      for (SchemaPath column : projectedColumns ) {
        String fieldName = column.getRootSegment().getPath();
        transformed.add(column);
        this.fields.put(fieldName, Integer.valueOf(1));
      }
    } else {
      // Tale all the fields including the _id
      this.fields.remove(DrillMongoConstants.ID);
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
    List<String> hosts = subScanSpec.getHosts();
    List<ServerAddress> addresses = Lists.newArrayList();
    for (String host : hosts) {
      addresses.add(new ServerAddress(host));
    }
    MongoClient client = plugin.getClient(addresses);
    MongoDatabase db = client.getDatabase(subScanSpec.getDbName());
    collection = db.getCollection(subScanSpec.getCollectionName());
  }

  @Override
  public void setup(OperatorContext context, OutputMutator output) throws ExecutionSetupException {
    this.operatorContext = context;
    this.writer = new VectorContainerWriter(output);
    this.jsonReader = new JsonReader(fragmentContext.getManagedBuffer(), Lists.newArrayList(getColumns()), enableAllTextMode, false, readNumbersAsDouble);

  }

  @Override
  public int next() {
    if(cursor == null){
      logger.info("Filters Applied : " + filters);
      logger.info("Fields Selected :" + fields);
      cursor = collection.find(filters).projection(fields).batchSize(100).iterator();
    }


    writer.allocate();
    writer.reset();

    int docCount = 0;
    Stopwatch watch = new Stopwatch();
    watch.start();

    try {
      while (docCount < BaseValueVector.INITIAL_VALUE_ALLOCATION && cursor.hasNext()) {
        writer.setPosition(docCount);
        String doc = cursor.next().toJson();
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
  public void close() {
  }


}
