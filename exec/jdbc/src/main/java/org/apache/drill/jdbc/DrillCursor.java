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

import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.RecordBatchLoader;
import org.apache.drill.exec.rpc.RpcException;
import org.apache.drill.exec.rpc.user.QueryDataBatch;

public class DrillCursor implements Cursor {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DrillCursor.class);

  private static final String UNKNOWN = "--UNKNOWN--";

  /** The associated java.sql.ResultSet implementation. */
  private final DrillResultSet resultSet;

  private final RecordBatchLoader currentBatch;
  private final DrillResultSet.ResultsListener resultsListener;

  // TODO:  Doc.:  Say what's started (set of rows?  just current result batch?)
  private boolean started = false;
  private boolean finished = false;
  // TODO:  Doc.: Say what "readFirstNext" means.
  private boolean redoFirstNext = false;
  // TODO:  Doc.: First what? (First batch? record? "next" call/operation?)
  private boolean first = true;

  private DrillColumnMetaDataList columnMetaDataList;
  private BatchSchema schema;

  /** Zero-based index of current record in record batch. */
  private int currentRecordNumber = -1;
  private long recordBatchCount;
  private final DrillAccessorList accessors = new DrillAccessorList();


  /**
   *
   * @param  resultSet  the associated ResultSet implementation
   */
  public DrillCursor(final DrillResultSet resultSet) {
    this.resultSet = resultSet;
    currentBatch = resultSet.currentBatch;
    resultsListener = resultSet.resultslistener;
  }

  public DrillResultSet getResultSet() {
    return resultSet;
  }

  protected int getCurrentRecordNumber() {
    return currentRecordNumber;
  }

  @Override
  public List<Accessor> createAccessors(List<ColumnMetaData> types, Calendar localCalendar, Factory factory) {
    columnMetaDataList = (DrillColumnMetaDataList) types;
    return accessors;
  }

  // TODO:  Doc.:  Specify what the return value actually means.  (The wording
  // "Moves to the next row" and "Whether moved" from the documentation of the
  // implemented interface (net.hydromatic.avatica.Cursor) doesn't address
  // moving past last row or how to evaluate "whether moved" on the first call.
  // In particular, document what the return value indicates about whether we're
  // currently at a valid row (or whether next() can be called again, or
  // whatever it does indicate), especially the first time this next() called
  // for a new result.
  @Override
  public boolean next() throws SQLException {
    if (!started) {
      started = true;
      redoFirstNext = true;
    } else if (redoFirstNext && !finished) {
      redoFirstNext = false;
      return true;
    }

    if (finished) {
      return false;
    }

    if (currentRecordNumber + 1 < currentBatch.getRecordCount()) {
      // Next index is in within current batch--just increment to that record.
      currentRecordNumber++;
      return true;
    } else {
      // Next index is not in current batch (including initial empty batch--
      // (try to) get next batch.
      try {
        QueryDataBatch qrb = resultsListener.getNext();
        recordBatchCount++;
        while (qrb != null && qrb.getHeader().getRowCount() == 0 && !first) {
          qrb.release();
          qrb = resultsListener.getNext();
          recordBatchCount++;
        }

        first = false;

        if (qrb == null) {
          currentBatch.clear();
          finished = true;
          return false;
        } else {
          currentRecordNumber = 0;
          boolean changed = currentBatch.load(qrb.getHeader().getDef(), qrb.getData());
          qrb.release();
          schema = currentBatch.getSchema();
          if (changed) {
            updateColumns();
          }
          if (redoFirstNext && currentBatch.getRecordCount() == 0) {
            redoFirstNext = false;
          }
          return true;
        }
      } catch (RpcException | InterruptedException | SchemaChangeException e) {
        throw new SQLException("Failure while executing query.", e);
      }

    }
  }

  void updateColumns() {
    accessors.generateAccessors(this, currentBatch);
    columnMetaDataList.updateColumnMetaData(UNKNOWN, UNKNOWN, UNKNOWN, schema);
    if (getResultSet().changeListener != null) {
      getResultSet().changeListener.schemaChanged(schema);
    }
  }

  public long getRecordBatchCount() {
    return recordBatchCount;
  }

  @Override
  public void close() {
    // currentBatch is owned by resultSet and cleaned up by
    // DrillResultSet.cleanup()

    // listener is owned by resultSet and cleaned up by
    // DrillResultSet.cleanup()

    // Clean up result set (to deallocate any buffers).
    getResultSet().cleanup();
    // TODO:  CHECK:  Something might need to set statement.openResultSet to
    // null.  Also, AvaticaResultSet.close() doesn't check whether already
    // closed and skip calls to cursor.close(), statement.onResultSetClose()
  }

  @Override
  public boolean wasNull() throws SQLException {
    return accessors.wasNull();
  }

}
