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
package org.apache.drill.jdbc.impl;

import static org.slf4j.LoggerFactory.getLogger;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.drill.exec.rpc.user.BlockingResultsListener;
import org.apache.drill.jdbc.DrillStatement;
import com.google.common.base.Stopwatch;
import org.apache.calcite.avatica.AvaticaStatement;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.Meta.Signature;
import org.apache.calcite.avatica.util.ArrayImpl.Factory;
import org.apache.calcite.avatica.util.Cursor;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.client.DrillClient;
import org.apache.drill.exec.proto.UserBitShared.QueryType;
import org.apache.drill.exec.proto.UserProtos.PreparedStatement;
import org.apache.drill.exec.proto.helper.QueryIdHelper;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.RecordBatchLoader;
import org.apache.drill.exec.rpc.user.QueryDataBatch;
import org.apache.drill.exec.store.ischema.InfoSchemaConstants;
import org.apache.drill.jdbc.SchemaChangeListener;
import org.slf4j.Logger;

public class DrillCursor implements Cursor {

  private static final Logger logger = getLogger(DrillCursor.class);

  /** JDBC-specified string for unknown catalog, schema, and table names. */
  private static final String UNKNOWN_NAME_STRING = "";

  private final DrillConnectionImpl connection;
  private final AvaticaStatement statement;
  private final Meta.Signature signature;

  /** Holds current batch of records (none before first load). */
  private final RecordBatchLoader currentBatchHolder;

  private final BlockingResultsListener resultsListener;
  private SchemaChangeListener changeListener;

  private final DrillAccessorList accessors = new DrillAccessorList();

  /** Schema of current batch (null before first load). */
  private BatchSchema schema;

  /** ... corresponds to current schema. */
  private DrillColumnMetaDataList columnMetaDataList;

  /** Whether loadInitialSchema() has been called. */
  private boolean initialSchemaLoaded;

  /** Whether after first batch.  (Re skipping spurious empty batches.) */
  private boolean afterFirstBatch;

  /**
   * Whether the next call to {@code this.}{@link #next()} should just return
   * {@code true} rather than calling {@link #nextRowInternally()} to try to
   * advance to the next record.
   * <p>
   *   Currently, can be true only for first call to {@link #next()}.
   * </p>
   * <p>
   *   (Relates to {@link #loadInitialSchema()}'s calling
   *   {@link #nextRowInternally()} one "extra" time (extra relative to number
   *   of {@link java.sql.ResultSet#next()} calls) at the beginning to get first batch
   *   and schema before {@code Statement.execute...(...)} even returns.)
   * </p>
   */
  private boolean returnTrueForNextCallToNext;

  /** Whether cursor is after the end of the sequence of records/rows. */
  private boolean afterLastRow;

  private int currentRowNumber = -1;
  /** Zero-based offset of current record in record batch.
   * (Not <i>row</i> number.) */
  private int currentRecordNumber = -1;

  //Track timeout period
  private long timeoutInMilliseconds;
  private Stopwatch elapsedTimer;

  /**
   *
   * @param statement
   * @param signature
   * @throws SQLException
   */
  DrillCursor(DrillConnectionImpl connection, AvaticaStatement statement, Signature signature) throws SQLException {
    this.connection = connection;
    this.statement = statement;
    this.signature = signature;

    DrillClient client = connection.getClient();
    final int batchQueueThrottlingThreshold =
        client.getConfig().getInt(
            ExecConstants.JDBC_BATCH_QUEUE_THROTTLING_THRESHOLD);
    resultsListener = new BlockingResultsListener(this::getElapsedTimer,
      this::getTimeoutInMilliseconds,
      batchQueueThrottlingThreshold);
    currentBatchHolder = new RecordBatchLoader(client.getAllocator());

    // Set Query Timeout
    logger.debug("Setting timeout as {}", this.statement.getQueryTimeout());
    setTimeout(this.statement.getQueryTimeout());
  }

  protected int getCurrentRecordNumber() {
    return currentRecordNumber;
  }

  public String getQueryId() {
    if (resultsListener.getQueryId() != null) {
      return QueryIdHelper.getQueryId(resultsListener.getQueryId());
    } else {
      return null;
    }
  }

  public boolean isBeforeFirst() {
    return currentRowNumber < 0;
  }

  public boolean isAfterLast() {
    return afterLastRow;
  }

  // (Overly restrictive Avatica uses List<Accessor> instead of List<? extends
  // Accessor>, so accessors/DrillAccessorList can't be of type
  // List<AvaticaDrillSqlAccessor>, and we have to cast from Accessor to
  // AvaticaDrillSqlAccessor in updateColumns().)
  @Override
  public List<Accessor> createAccessors(List<ColumnMetaData> types,
                                        Calendar localCalendar, Factory factory) {
    columnMetaDataList = (DrillColumnMetaDataList) types;
    return accessors;
  }

  synchronized void cleanup() {
    if (resultsListener.getQueryId() != null && ! resultsListener.isCompleted()) {
      connection.getClient().cancelQuery(resultsListener.getQueryId());
    }
    resultsListener.close();
    currentBatchHolder.clear();
  }

  long getTimeoutInMilliseconds() {
    return timeoutInMilliseconds;
  }

  //Set the cursor's timeout in seconds
  void setTimeout(int timeoutDurationInSeconds) {
    this.timeoutInMilliseconds = TimeUnit.SECONDS.toMillis(timeoutDurationInSeconds);
    //Starting the timer, since this is invoked via the ResultSet.execute() call
    if (timeoutInMilliseconds > 0) {
      elapsedTimer = Stopwatch.createStarted();
    }
  }

  /**
   * Updates column accessors and metadata from current record batch.
   */
  private void updateColumns() {
    // First update accessors and schema from batch:
    accessors.generateAccessors(this, currentBatchHolder);

    // Extract Java types from accessors for metadata's getColumnClassName:
    final List<Class<?>> getObjectClasses = new ArrayList<>();
    // (Can't use modern for loop because, for some incompletely clear reason,
    // DrillAccessorList blocks iterator() (throwing exception).)
    for (int ax = 0; ax < accessors.size(); ax++) {
      final AvaticaDrillSqlAccessor accessor =
          accessors.get(ax);
      getObjectClasses.add(accessor.getObjectClass());
    }

    // Update metadata for result set.
    columnMetaDataList.updateColumnMetaData(
        InfoSchemaConstants.IS_CATALOG_NAME,
        UNKNOWN_NAME_STRING,  // schema name
        UNKNOWN_NAME_STRING,  // table name
        schema,
        getObjectClasses);

    if (changeListener != null) {
      changeListener.schemaChanged(schema);
    }
  }

  /**
   * ...
   * <p>
   *   Is to be called (once) from {@link #loadInitialSchema} for
   *   {@link DrillResultSetImpl#execute()}, and then (repeatedly) from
   *   {@link #next()} for {@link org.apache.calcite.avatica.AvaticaResultSet#next()}.
   * </p>
   *
   * @return  whether cursor is positioned at a row (false when after end of
   *   results)
   */
  private boolean nextRowInternally() throws SQLException {
    if (currentRecordNumber + 1 < currentBatchHolder.getRecordCount()) {
      // Have next row in current batch--just advance index and report "at a row."
      currentRecordNumber++;
      return true;
    } else {
      // No (more) records in any current batch--try to get first or next batch.
      // (First call always takes this branch.)

      try {
        QueryDataBatch qrb = resultsListener.getNext();

        // (Apparently:)  Skip any spurious empty batches (batches that have
        // zero rows and null data, other than the first batch (which carries
        // the (initial) schema but no rows)).
        if (afterFirstBatch) {
          while (qrb != null
              && (qrb.getHeader().getRowCount() == 0 && qrb.getData() == null)) {
            // Empty message--dispose of and try to get another.
            logger.warn("Spurious batch read: {}", qrb);

            qrb.release();

            qrb = resultsListener.getNext();
          }
        }

        afterFirstBatch = true;

        if (qrb == null) {
          // End of batches--clean up, set state to done, report after last row.

          currentBatchHolder.clear();  // (We load it so we clear it.)
          afterLastRow = true;
          return false;
        } else {
          // Got next (or first) batch--reset record offset to beginning;
          // assimilate schema if changed; set up return value for first call
          // to next().

          currentRecordNumber = 0;

          if (qrb.getHeader().hasAffectedRowsCount()) {
            int updateCount = qrb.getHeader().getAffectedRowsCount();
            int currentUpdateCount = statement.getUpdateCount() == -1 ? 0 : statement.getUpdateCount();
            ((DrillStatement) statement).setUpdateCount(updateCount + currentUpdateCount);
            ((DrillStatement) statement).setResultSet(null);
          }

          final boolean schemaChanged;
          try {
            schemaChanged = currentBatchHolder.load(qrb.getHeader().getDef(), qrb.getData());
          }
          finally {
            qrb.release();
          }
          schema = currentBatchHolder.getSchema();
          if (schemaChanged) {
            updateColumns();
          }

          if (returnTrueForNextCallToNext
              && currentBatchHolder.getRecordCount() == 0) {
            returnTrueForNextCallToNext = false;
          }
          return true;
        }
      }
      catch (UserException e) {
        // A normally expected case--for any server-side error (e.g., syntax
        // error in SQL statement).
        // Construct SQLException with message text from the UserException.
        // TODO:  Map UserException error type to SQLException subclass (once
        // error type is accessible, of course. :-()
        throw new SQLException(e.getMessage(), e);
      }
      catch (InterruptedException e) {
        // Not normally expected--Drill doesn't interrupt in this area (right?)--
        // but JDBC client certainly could.
        throw new SQLException("Interrupted.", e);
      }
      catch (RuntimeException e) {
        throw new SQLException("Unexpected RuntimeException: " + e.toString(), e);
      }
    }
  }

  /**
   * Advances to first batch to load schema data into result set metadata.
   * <p>
   *   To be called once from {@link DrillResultSetImpl#execute()} before
   *   {@link #next()} is called from {@link org.apache.calcite.avatica.AvaticaResultSet#next()}.
   * <p>
   */
  void loadInitialSchema() throws SQLException {
    if (initialSchemaLoaded) {
      throw new IllegalStateException(
          "loadInitialSchema() called a second time");
    }

    assert ! afterLastRow : "afterLastRow already true in loadInitialSchema()";
    assert ! afterFirstBatch : "afterLastRow already true in loadInitialSchema()";
    assert -1 == currentRecordNumber
        : "currentRecordNumber not -1 (is " + currentRecordNumber
          + ") in loadInitialSchema()";
    assert 0 == currentBatchHolder.getRecordCount()
        : "currentBatchHolder.getRecordCount() not 0 (is "
          + currentBatchHolder.getRecordCount() + " in loadInitialSchema()";

    final PreparedStatement preparedStatement;
    if (statement instanceof DrillPreparedStatementImpl) {
      DrillPreparedStatementImpl drillPreparedStatement = (DrillPreparedStatementImpl) statement;
      preparedStatement = drillPreparedStatement.getPreparedStatementHandle();
    } else {
      preparedStatement = null;
    }

    if (preparedStatement != null) {
      connection.getClient().executePreparedStatement(preparedStatement.getServerHandle(), resultsListener);
    }
    else {
      connection.getClient().runQuery(QueryType.SQL, signature.sql, resultsListener);
    }

    try {
      resultsListener.awaitFirstMessage();
    } catch (InterruptedException e) {
      // Preserve evidence that the interruption occurred so that code higher up
      // on the call stack can learn of the interruption and respond to it if it
      // wants to.
      Thread.currentThread().interrupt();

      // Not normally expected--Drill doesn't interrupt in this area (right?)--
      // but JDBC client certainly could.
      throw new SQLException("Interrupted", e);
    }

    returnTrueForNextCallToNext = true;

    nextRowInternally();

    initialSchemaLoaded = true;
  }

  /**
   * Advances this cursor to the next row, if any, or to after the sequence of
   * rows if no next row.
   *
   * @return  whether cursor is positioned at a row (false when after end of
   *   results)
   */
  @Override
  public boolean next() throws SQLException {
    if (! initialSchemaLoaded) {
      throw new IllegalStateException(
          "next() called but loadInitialSchema() was not called");
    }
    assert afterFirstBatch : "afterFirstBatch still false in next()";

    if (afterLastRow) {
      // We're already after end of rows/records--just report that after end.
      return false;
    }
    else if (returnTrueForNextCallToNext) {
      ++currentRowNumber;
      // We have a deferred "not after end" to report--reset and report that.
      returnTrueForNextCallToNext = false;
      return true;
    }
    else {
      accessors.clearLastColumnIndexedInRow();
      boolean res = nextRowInternally();
      if (res) { ++ currentRowNumber; }

      return res;
    }
  }

  public void cancel() {
    close();
  }

  @Override
  public void close() {
    // Clean up result set (to deallocate any buffers).
    cleanup();
    // TODO:  CHECK:  Something might need to set statement.openResultSet to
    // null.  Also, AvaticaResultSet.close() doesn't check whether already
    // closed and skip calls to cursor.close(), statement.onResultSetClose()
  }

  @Override
  public boolean wasNull() throws SQLException {
    return accessors.wasNull();
  }

  public Stopwatch getElapsedTimer() {
    return elapsedTimer;
  }
}
