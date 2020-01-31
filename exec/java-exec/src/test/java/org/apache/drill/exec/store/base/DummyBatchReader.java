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
package org.apache.drill.exec.store.base;

import java.util.List;

import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.physical.impl.scan.framework.ManagedReader;
import org.apache.drill.exec.physical.impl.scan.framework.SchemaNegotiator;
import org.apache.drill.exec.physical.resultSet.ResultSetLoader;
import org.apache.drill.exec.physical.resultSet.RowSetLoader;
import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.store.base.filter.RelOp;

public class DummyBatchReader implements ManagedReader<SchemaNegotiator> {

  @SuppressWarnings("unused")
  private final DummyStoragePluginConfig config;
  @SuppressWarnings("unused")
  private final List<SchemaPath> columns;

  // Filters are not actually used; just carried along for testing
  @SuppressWarnings("unused")
  private final List<RelOp> filters;
  private ResultSetLoader rsLoader;

  public DummyBatchReader(DummyStoragePluginConfig config,
      List<SchemaPath> columns, List<RelOp> filters) {
    this.config = config;
    this.columns = columns;
    this.filters = filters;
  }

  @Override
  public boolean open(SchemaNegotiator negotiator) {
    TupleMetadata schema = new SchemaBuilder()
          .add("a", MinorType.VARCHAR)
          .add("b", MinorType.VARCHAR)
          .add("c", MinorType.VARCHAR)
          .build();
    negotiator.setTableSchema(schema, true);
    rsLoader = negotiator.build();
    return true;
  }

  @Override
  public boolean next() {
    if (rsLoader.batchCount() > 0) {
      return false;
    }
    RowSetLoader writer = rsLoader.writer();
    int rowCount = 3;
    int colCount = writer.tupleSchema().size();
    for (int i = 0; i < rowCount; i++) {
      writer.start();
      for (int j = 0; j < colCount; j++) {
        writer.scalar(j).setString("Row " + (i + 1) + ", Col " + (j + 1));
      }
      writer.save();
    }
    return true;
  }

  @Override
  public void close() { }
}
