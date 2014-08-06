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
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.PathSegment.NameSegment;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.store.RecordReader;
import org.apache.drill.exec.vector.complex.fn.JsonReaderWithState;
import org.apache.drill.exec.vector.complex.impl.VectorContainerWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Sets;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;

public class MongoRecordReader implements RecordReader {
  static final Logger logger = LoggerFactory.getLogger(MongoRecordReader.class);

  private static final int TARGET_RECORD_COUNT = 4000;

  private LinkedHashSet<SchemaPath> columns;

  private DBCollection collection;
  private DBCursor cursor;

  private JsonReaderWithState jsonReader;
  private VectorContainerWriter writer;

  private DBObject filters;
  private DBObject fields;

  private MongoClient client;

  public MongoRecordReader(MongoSubScan.MongoSubScanSpec subScanSpec, List<SchemaPath> projectedColumns,
      FragmentContext context) {
    this.columns = Sets.newLinkedHashSet();
    this.fields = new BasicDBObject();
    if (projectedColumns != null && projectedColumns.size() != 0) {
      Iterator<SchemaPath> columnIterator = projectedColumns.iterator();
      while (columnIterator.hasNext()) {
        SchemaPath column = columnIterator.next();
        NameSegment root = column.getRootSegment();
        String fieldName = root.getPath();
        this.columns.add(SchemaPath.getSimplePath(fieldName));
        logger.debug("Field selected : " + fieldName);
        this.fields.put(fieldName, Integer.valueOf(1));
      }
    }
    this.filters = new BasicDBObject();
    // exclude _id field, if not mentioned by user.
    this.fields.put(DrillMongoConstants.ID, Integer.valueOf(0));
    init(subScanSpec);
  }

  private void init(MongoSubScan.MongoSubScanSpec subScanSpec) {
    try {
      client = new MongoClient(subScanSpec.getShard(), subScanSpec.getPort());
      DB db = client.getDB(subScanSpec.getDbName());
      collection = db.getCollection(subScanSpec.getCollectionName());
    } catch (UnknownHostException e) {
      throw new DrillRuntimeException(e.getMessage(), e);
    }
  }

  @Override
  public void setup(OutputMutator output) throws ExecutionSetupException {
    try {
      this.writer = new VectorContainerWriter(output);
      this.jsonReader = new JsonReaderWithState();
    } catch (IOException e) {
      throw new ExecutionSetupException("Failure in Mongo JsonReader initialization.", e);
    }
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
    int rowCount = 0;

    try {
      done: for (; rowCount < TARGET_RECORD_COUNT && cursor.hasNext(); rowCount++) {
        writer.setPosition(docCount);
        DBObject record = cursor.next();
        if (record == null) {
          break done;
        }

        switch (jsonReader.write(record.toString().getBytes(Charsets.UTF_8), writer)) {
        case WRITE_SUCCEED:
          docCount++;
          break;

        case NO_MORE:
          break done;

        case WRITE_FAILED:
          if (docCount == 0) {
            throw new DrillRuntimeException("Record is too big to fit into allocated ValueVector");
          }
          break done;
        }
      }
      writer.setValueCount(docCount);
      logger.debug("Took {} ms to get {} records", watch.elapsed(TimeUnit.MILLISECONDS), rowCount);
      return docCount;
    } catch (Exception e) {
      throw new DrillRuntimeException("Failure while reading Mongo Record.", e);
    }
  }

  @Override
  public void cleanup() {
    client.close();
  }
}
