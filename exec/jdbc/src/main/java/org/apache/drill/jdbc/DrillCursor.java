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
package org.apache.drill.jdbc;

import java.sql.SQLException;
import java.util.Calendar;
import java.util.List;

import net.hydromatic.avatica.ArrayImpl.Factory;
import net.hydromatic.avatica.ColumnMetaData;
import net.hydromatic.avatica.Cursor;
import net.hydromatic.avatica.Cursor.Accessor;

import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.RecordBatchLoader;
import org.apache.drill.exec.rpc.RpcException;
import org.apache.drill.exec.rpc.user.QueryResultBatch;

public class DrillCursor implements Cursor{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DrillCursor.class);

  private static final String UNKNOWN = "--UNKNOWN--";

  private boolean started = false;
  private boolean finished = false;
  private final RecordBatchLoader currentBatch;
  private final DrillResultSet.Listener listener;
  private boolean redoFirstNext = false;
  private boolean first = true;

  private DrillColumnMetaDataList columnMetaDataList;
  private BatchSchema schema;

  final DrillResultSet results;
  int currentRecord = 0;
  private long recordBatchCount;
  private final DrillAccessorList accessors = new DrillAccessorList();


  public DrillCursor(DrillResultSet results) {
    super();
    this.results = results;
    currentBatch = results.currentBatch;
    this.listener = results.listener;
  }

  @Override
  public List<Accessor> createAccessors(List<ColumnMetaData> types, Calendar localCalendar, Factory factory) {
    columnMetaDataList = (DrillColumnMetaDataList) types;
    return accessors;
  }

  @Override
  public boolean next() throws SQLException {
    if(!started){
      started = true;
      redoFirstNext = true;
    }else if(redoFirstNext && !finished){
      redoFirstNext = false;
      return true;
    }

    if(finished) return false;

    if(currentRecord+1 < currentBatch.getRecordCount()){
      currentRecord++;
      return true;
    }else{
      try {
        QueryResultBatch qrb = listener.getNext();
        recordBatchCount++;
        while(qrb != null && qrb.getHeader().getRowCount() == 0 && !first){
          qrb.release();
          qrb = listener.getNext();
          recordBatchCount++;
        }

        first = false;

        if(qrb == null){
          finished = true;
          return false;
        }else{
          currentRecord = 0;
          boolean changed = currentBatch.load(qrb.getHeader().getDef(), qrb.getData());
          schema = currentBatch.getSchema();
          if(changed) updateColumns();
          if (redoFirstNext && currentBatch.getRecordCount() == 0) {
            redoFirstNext = false;
          }
          return true;
        }
      } catch (RpcException | InterruptedException | SchemaChangeException e) {
        throw new SQLException("Failure while trying to get next result batch.", e);
      }

    }
  }

  void updateColumns(){
    accessors.generateAccessors(this, currentBatch);
    columnMetaDataList.updateColumnMetaData(UNKNOWN, UNKNOWN, UNKNOWN, schema);
    if(results.changeListener != null) results.changeListener.schemaChanged(schema);
  }

  public long getRecordBatchCount(){
    return recordBatchCount;
  }

  @Override
  public void close() {
    results.cleanup();
  }

  @Override
  public boolean wasNull() throws SQLException {
    return accessors.wasNull();
  }

}
