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
package org.apache.drill.exec.vector.complex.fn;


import java.util.List;
import java.util.Objects;

import org.apache.drill.BaseTestQuery;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.proto.UserBitShared;
import org.apache.drill.exec.proto.beans.QueryResult;
import org.apache.drill.exec.record.RecordBatchLoader;
import org.apache.drill.exec.rpc.user.QueryResultBatch;
import org.apache.drill.exec.vector.ValueVector;
import org.junit.Test;

public class JsonReaderTests extends BaseTestQuery {

  static interface Function<T> {
    void apply(T param);
  }

  protected void query(final String query, final Function<RecordBatchLoader> testBody) throws Exception {
    List<QueryResultBatch> batches = testSqlWithResults(query);
    RecordBatchLoader loader = new RecordBatchLoader(client.getAllocator());
    try {
      QueryResultBatch batch = batches.get(0);
      loader.load(batch.getHeader().getDef(), batch.getData());

      testBody.apply(loader);

    } finally {
      for (QueryResultBatch batch:batches) {
        batch.release();
      }
      loader.clear();
    }
  }

  @Test
  public void testIfDrillCanReadSparseRecords() throws Exception {
    final String sql = "select * from cp.`vector/complex/fn/sparse.json`";
    query(sql, new Function<RecordBatchLoader>() {
      @Override
      public void apply(RecordBatchLoader loader) {
        assert loader.getRecordCount() == 4 : "invalid record count returned";

        //XXX: make sure value order matches vector order
        final Object[][] values = new Object[][] {
            {null, null},
            {1L, null},
            {null, 2L},
            {3L, 3L}
        };

        Object[] row;
        Object expected;
        Object actual;
        for (int r=0;r<values.length;r++) {
          row = values[r];
          for (int c=0; c<values[r].length; c++) {
            expected = row[c];
            actual = loader.getValueAccessorById(ValueVector.class, c).getValueVector().getAccessor().getObject(r);
            assert Objects.equals(expected, actual) : String.format("row:%d - col:%d - expected:%s[%s] - actual:%s[%s]",
                r, c,
                expected,
                expected==null?"null":expected.getClass().getSimpleName(),
                actual,
                actual==null?"null":actual.getClass().getSimpleName());
          }
        }
      }
    });
  }

  @Test
  public void testIfDrillCanReadSparseNestedRecordsWithoutRaisingException() throws Exception {
    final String sql = "select * from cp.`vector/complex/fn/nested-with-nulls.json`";
    query(sql, new Function<RecordBatchLoader>() {
      @Override
      public void apply(RecordBatchLoader loader) {
        assert loader.getRecordCount() == 3 : "invalid record count returned";

        //XXX: make sure value order matches vector order
        final Object[][] values = new Object[][] {
            {"[{},{},{},{\"name\":\"doe\"},{}]"},
            {"[]"},
            {"[{\"name\":\"john\",\"id\":10}]"},
        };

        Object[] row;
        Object expected;
        Object actual;
        for (int r=0;r<values.length;r++) {
          row = values[r];
          for (int c = 0; c < values[r].length; c++) {
            expected = row[c];
            actual = loader.getValueAccessorById(ValueVector.class, c).getValueVector().getAccessor().getObject(r);
            assert Objects.equals(actual, expected) : String.format("row:%d - col:%d - expected:%s[%s] - actual:%s[%s]",
                r, c,
                expected,
                expected == null ? "null" : expected.getClass().getSimpleName(),
                actual,
                actual == null ? "null" : actual.getClass().getSimpleName());
          }
        }
      }
    });
  }

}
