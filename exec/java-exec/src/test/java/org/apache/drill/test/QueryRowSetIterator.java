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
package org.apache.drill.test;

import java.util.Iterator;

import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.proto.UserBitShared.QueryId;
import org.apache.drill.exec.proto.UserBitShared.QueryResult.QueryState;
import org.apache.drill.exec.proto.helper.QueryIdHelper;
import org.apache.drill.exec.record.RecordBatchLoader;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.rpc.user.QueryDataBatch;
import org.apache.drill.test.BufferingQueryEventListener.QueryEvent;
import org.apache.drill.test.rowSet.DirectRowSet;

public class QueryRowSetIterator implements Iterator<DirectRowSet>, Iterable<DirectRowSet> {
  private final BufferingQueryEventListener listener;
  private int recordCount = 0;
  private int batchCount = 0;
  QueryId queryId = null;
  private BufferAllocator allocator;
  private QueryDataBatch batch;
  private QueryState state;

  QueryRowSetIterator(BufferAllocator allocator, BufferingQueryEventListener listener) {
    this.allocator = allocator;
    this.listener = listener;
  }

  public QueryId queryId() { return queryId; }
  public String queryIdString() { return QueryIdHelper.getQueryId(queryId); }
  public QueryState finalState() { return state; }
  public int batchCount() { return batchCount; }
  public int rowCount() { return recordCount; }

  @Override
  public boolean hasNext() {
    for (;;) {
      QueryEvent event = listener.get();
      state = event.state;
      batch = null;
      switch (event.type)
      {
      case BATCH:
        batchCount++;
        recordCount += event.batch.getHeader().getRowCount();
        batch = event.batch;
        return true;
      case EOF:
        state = event.state;
        return false;
      case ERROR:
        throw new RuntimeException(event.error);
      case QUERY_ID:
        queryId = event.queryId;
        break;
      default:
        throw new IllegalStateException("Unexpected event: " + event.type);
      }
    }
  }

  @Override
  public DirectRowSet next() {

    if (batch == null) {
      throw new IllegalStateException();
    }

    // Unload the batch and convert to a row set.

    final RecordBatchLoader loader = new RecordBatchLoader(allocator);
    try {
      loader.load(batch.getHeader().getDef(), batch.getData());
      batch.release();
      batch = null;
      VectorContainer container = loader.getContainer();
      container.setRecordCount(loader.getRecordCount());
      return DirectRowSet.fromContainer(container);
    } catch (SchemaChangeException e) {
      throw new IllegalStateException(e);
    }
  }

  public void printAll() {
    for (DirectRowSet rowSet : this) {
      rowSet.print();
      rowSet.clear();
    }
  }

  @Override
  public Iterator<DirectRowSet> iterator() {
    return this;
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException();
  }
}
