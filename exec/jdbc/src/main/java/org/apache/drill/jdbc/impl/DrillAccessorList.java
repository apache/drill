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
package org.apache.drill.jdbc.impl;

import java.sql.SQLException;

import net.hydromatic.avatica.Cursor.Accessor;

import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.record.RecordBatchLoader;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.accessor.BoundCheckingAccessor;
import org.apache.drill.exec.vector.accessor.SqlAccessor;


class DrillAccessorList extends BasicList<Accessor>{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DrillAccessorList.class);

  private AvaticaDrillSqlAccessor[] accessors = new AvaticaDrillSqlAccessor[0];
  // TODO  Rename to lastColumnAccessed and/or document.
  // TODO  Why 1, rather than, say, -1?
  private int lastColumn = 1;

  void generateAccessors(DrillCursor cursor, RecordBatchLoader currentBatch) {
    int cnt = currentBatch.getSchema().getFieldCount();
    accessors = new AvaticaDrillSqlAccessor[cnt];
    for(int i =0; i < cnt; i++){
      final ValueVector vector = currentBatch.getValueAccessorById(null, i).getValueVector();
      final SqlAccessor acc =
          new TypeConvertingSqlAccessor(
              new BoundCheckingAccessor(vector, TypeHelper.getSqlAccessor(vector))
              );
      accessors[i] = new AvaticaDrillSqlAccessor(acc, cursor);
    }
  }

  @Override
  public AvaticaDrillSqlAccessor get(int index) {
    lastColumn = index;
    return accessors[index];
  }

  boolean wasNull() throws SQLException{
    return accessors[lastColumn].wasNull();
  }

  @Override
  public int size() {
    return accessors.length;
  }

}
