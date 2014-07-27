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
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;

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
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

public class MongoRecordReader implements RecordReader {
  static final Logger logger = LoggerFactory.getLogger(MongoRecordReader.class);
  
  private static final int TARGET_RECORD_COUNT = 4000;
  
  private LinkedHashSet<SchemaPath> columns;
  private OutputMutator outputMutator;
  
  private DBCollection collection;
  private DBObject leftOver;
  private DBCursor cursor;
  
  private MongoGroupScan scan;
  
  private JsonReaderWithState jsonReader;
  private OutputMutator mutator;
  private VectorContainerWriter writer;

  public MongoRecordReader(MongoGroupScan scan, List<SchemaPath> projectedColumns, FragmentContext context) {
    this.scan = scan;
    this.columns = Sets.newLinkedHashSet();
    if (projectedColumns != null && projectedColumns.size() != 0) {
      Iterator<SchemaPath> columnIterator = projectedColumns.iterator();
      while (columnIterator.hasNext()) {
        SchemaPath column = columnIterator.next();
        NameSegment root = column.getRootSegment();
        this.columns.add(SchemaPath.getSimplePath(root.getPath()));
      }
    }
    
    // set the projected fields for create the query.
    cursor = collection.find();
  }

  @Override
  public void setup(OutputMutator output) throws ExecutionSetupException {
    this.outputMutator = output;
    for (SchemaPath column : columns) {
      getOrCreateVector(column.getRootSegment().getPath(), false);
    }
    collection = scan.getCollection();
    try {
      this.writer = new VectorContainerWriter(output);
      this.mutator = output;
      jsonReader = new JsonReaderWithState();
    } catch (IOException e) {
      throw new ExecutionSetupException(
          "Failure in Mongo JsonReader initialization.", e);
    }
  }

  private void getOrCreateVector(String path, boolean b) {

  }

  @Override
  public int next() {
    writer.allocate();
    writer.reset();

    int i = 0;
    Stopwatch watch = new Stopwatch();
    watch.start();
    int rowCount = 0;

    try {
      done: for (; rowCount < TARGET_RECORD_COUNT; rowCount++) {
        writer.setPosition(i);
        DBObject record = cursor.next();
        if (leftOver != null) {
          record = leftOver;
          leftOver = null;
        } else {
          record = cursor.next();
        }

        if (record == null) {
          break done;
        }

        switch (jsonReader.write(record.toString().getBytes(Charsets.UTF_8),
            writer)) {
        case WRITE_SUCCEED:
          i++;
          break;

        case NO_MORE:
          break done;

        case WRITE_FAILED:
          if (i == 0) {
            throw new DrillRuntimeException(
                "Record is too big to fit into allocated ValueVector");
          }
          break done;
        }
        ;

      }
    } catch (IOException e) {
      throw new DrillRuntimeException("Failure while reading Mongo Record.", e);
    }
    writer.setValueCount(i);
    return i;
  }

  @Override
  public void cleanup() {

  }
}
