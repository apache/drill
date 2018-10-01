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
import org.apache.drill.exec.store.bson.BsonRecordReader;
import org.apache.drill.exec.vector.BaseValueVector;
import org.apache.drill.exec.vector.complex.fn.JsonReader;
import org.apache.drill.exec.vector.complex.impl.VectorContainerWriter;
import org.bson.BsonDocument;
import org.bson.BsonDocumentReader;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.drill.shaded.guava.com.google.common.base.Charsets;
import org.apache.drill.shaded.guava.com.google.common.base.Stopwatch;
import org.apache.drill.shaded.guava.com.google.common.collect.Lists;
import org.apache.drill.shaded.guava.com.google.common.collect.Sets;
import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;

public class MongoRecordReader extends AbstractRecordReader {
  private static final Logger logger = LoggerFactory.getLogger(MongoRecordReader.class);

  private MongoCollection<BsonDocument> collection;
  private MongoCursor<BsonDocument> cursor;

  private JsonReader jsonReader;
  private BsonRecordReader bsonReader;
  private VectorContainerWriter writer;

  private Document filters;
  private final Document fields;

  private final FragmentContext fragmentContext;

  private final MongoStoragePlugin plugin;

  private final boolean enableAllTextMode;
  private final boolean enableNanInf;
  private final boolean readNumbersAsDouble;
  private boolean unionEnabled;
  private final boolean isBsonRecordReader;

  public MongoRecordReader(MongoSubScan.MongoSubScanSpec subScanSpec, List<SchemaPath> projectedColumns,
      FragmentContext context, MongoStoragePlugin plugin) {

    fields = new Document();
    // exclude _id field, if not mentioned by user.
    fields.put(DrillMongoConstants.ID, Integer.valueOf(0));
    setColumns(projectedColumns);
    fragmentContext = context;
    this.plugin = plugin;
    filters = new Document();
    Map<String, List<Document>> mergedFilters = MongoUtils.mergeFilters(
        subScanSpec.getMinFilters(), subScanSpec.getMaxFilters());

    buildFilters(subScanSpec.getFilter(), mergedFilters);
    enableAllTextMode = fragmentContext.getOptions().getOption(ExecConstants.MONGO_ALL_TEXT_MODE).bool_val;
    enableNanInf = fragmentContext.getOptions().getOption(ExecConstants.JSON_READER_NAN_INF_NUMBERS).bool_val;
    readNumbersAsDouble = fragmentContext.getOptions().getOption(ExecConstants.MONGO_READER_READ_NUMBERS_AS_DOUBLE).bool_val;
    isBsonRecordReader = fragmentContext.getOptions().getOption(ExecConstants.MONGO_BSON_RECORD_READER).bool_val;
    logger.debug("BsonRecordReader is enabled? " + isBsonRecordReader);
    init(subScanSpec);
  }

  @Override
  protected Collection<SchemaPath> transformColumns(Collection<SchemaPath> projectedColumns) {
    Set<SchemaPath> transformed = Sets.newLinkedHashSet();
    if (!isStarQuery()) {
      for (SchemaPath column : projectedColumns) {
        String fieldName = column.getRootSegment().getPath();
        transformed.add(column);
        this.fields.put(fieldName, Integer.valueOf(1));
      }
    } else {
      // Tale all the fields including the _id
      this.fields.remove(DrillMongoConstants.ID);
      transformed.add(SchemaPath.STAR_COLUMN);
    }
    return transformed;
  }

  private void buildFilters(Document pushdownFilters,
      Map<String, List<Document>> mergedFilters) {
    for (Entry<String, List<Document>> entry : mergedFilters.entrySet()) {
      List<Document> list = entry.getValue();
      if (list.size() == 1) {
        this.filters.putAll(list.get(0));
      } else {
        Document andQueryFilter = new Document();
        andQueryFilter.put("$and", list);
        this.filters.putAll(andQueryFilter);
      }
    }
    if (pushdownFilters != null && !pushdownFilters.isEmpty()) {
      if (!mergedFilters.isEmpty()) {
        this.filters = MongoUtils.andFilterAtIndex(this.filters, pushdownFilters);
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
    this.unionEnabled = fragmentContext.getOptions().getBoolean(ExecConstants.ENABLE_UNION_TYPE_KEY);
    collection = db.getCollection(subScanSpec.getCollectionName(), BsonDocument.class);
  }

  @Override
  public void setup(OperatorContext context, OutputMutator output) throws ExecutionSetupException {
    this.writer = new VectorContainerWriter(output, unionEnabled);
    // Default is BsonReader and all text mode will not be honored in
    // BsonRecordReader
    if (isBsonRecordReader) {
      this.bsonReader = new BsonRecordReader(fragmentContext.getManagedBuffer(), Lists.newArrayList(getColumns()),
          readNumbersAsDouble);
      logger.debug("Initialized BsonRecordReader. ");
    } else {
      this.jsonReader = new JsonReader.Builder(fragmentContext.getManagedBuffer())
          .schemaPathColumns(Lists.newArrayList(getColumns()))
          .allTextMode(enableAllTextMode)
          .readNumbersAsDouble(readNumbersAsDouble)
          .enableNanInf(enableNanInf)
          .build();
      logger.debug(" Intialized JsonRecordReader. ");
    }
  }

  @Override
  public int next() {
    if (cursor == null) {
      logger.info("Filters Applied : " + filters);
      logger.info("Fields Selected :" + fields);
      cursor = collection.find(filters).projection(fields).batchSize(100).iterator();
    }

    writer.allocate();
    writer.reset();

    int docCount = 0;
    Stopwatch watch = Stopwatch.createStarted();

    try {
      while (docCount < BaseValueVector.INITIAL_VALUE_ALLOCATION && cursor.hasNext()) {
        writer.setPosition(docCount);
        if (isBsonRecordReader) {
          BsonDocument bsonDocument = cursor.next();
          bsonReader.write(writer, new BsonDocumentReader(bsonDocument));
        } else {
          String doc = cursor.next().toJson();
          jsonReader.setSource(doc.getBytes(Charsets.UTF_8));
          jsonReader.write(writer);
        }
        docCount++;
      }

      if (isBsonRecordReader) {
        bsonReader.ensureAtLeastOneField(writer);
      } else {
        jsonReader.ensureAtLeastOneField(writer);
      }

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
  public void close() {
  }

  @Override
  public String toString() {
    Object reader = isBsonRecordReader ? bsonReader : jsonReader;
    return "MongoRecordReader[reader=" + reader + "]";
  }
}
