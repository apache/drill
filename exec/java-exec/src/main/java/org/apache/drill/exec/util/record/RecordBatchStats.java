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
package org.apache.drill.exec.util.record;

import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.FragmentContextImpl;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.proto.ExecProtos.FragmentHandle;
import org.apache.drill.exec.proto.helper.QueryIdHelper;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.RecordBatchSizer;
import org.apache.drill.exec.record.RecordBatchSizer.ColumnSize;

/**
 * Utility class to capture key record batch statistics.
 */
public final class RecordBatchStats {
  /** A prefix for all batch stats to simplify search */
  public static final String BATCH_STATS_PREFIX = "BATCH_STATS";

  /** Helper class which loads contextual record batch logging options */
  public static final class RecordBatchStatsContext {
    private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(RecordBatchStatsContext.class);

    /** batch size logging for all readers */
    private final boolean enableBatchSzLogging;
    /** Fine grained batch size logging */
    private final boolean enableFgBatchSzLogging;
    /** Unique Operator Identifier */
    private final String contextOperatorId;

    /**
     * @param options options manager
     */
    public RecordBatchStatsContext(FragmentContext context, OperatorContext oContext) {
      enableBatchSzLogging   = context.getOptions().getOption(ExecConstants.STATS_LOGGING_BATCH_SZ_OPTION).bool_val;
      enableFgBatchSzLogging = context.getOptions().getOption(ExecConstants.STATS_LOGGING_FG_BATCH_SZ_OPTION).bool_val;
      contextOperatorId      = new StringBuilder()
        .append(getQueryId(context))
        .append(":")
        .append(oContext.getStats().getId())
        .toString();
    }

    /**
     * @return the enableBatchSzLogging
     */
    public boolean isEnableBatchSzLogging() {
      return enableBatchSzLogging || enableFgBatchSzLogging || logger.isDebugEnabled();
    }

    /**
     * @return the enableFgBatchSzLogging
     */
    public boolean isEnableFgBatchSzLogging() {
      return enableFgBatchSzLogging || logger.isDebugEnabled();
    }

    /**
     * @return indicates whether stats messages should be logged in info or debug level
     */
    public boolean useInfoLevelLogging() {
      return isEnableBatchSzLogging() && !logger.isDebugEnabled();
    }

    /**
     * @return the contextOperatorId
     */
    public String getContextOperatorId() {
      return contextOperatorId;
    }

    private String getQueryId(FragmentContext _context) {
      if (_context instanceof FragmentContextImpl) {
        final FragmentContextImpl context = (FragmentContextImpl) _context;
        final FragmentHandle handle       = context.getHandle();

        if (handle != null) {
          return QueryIdHelper.getQueryIdentifier(handle);
        }
      }
      return "NA";
    }
  }

  /**
   * Constructs record batch statistics for the input record batch
   *
   * @param stats instance identifier
   * @param sourceId optional source identifier for scanners
   * @param recordBatch a set of records
   * @param verbose whether to include fine-grained stats
   *
   * @return a string containing the record batch statistics
   */
  public static String printRecordBatchStats(String statsId,
    String sourceId,
    RecordBatch recordBatch,
    boolean verbose) {

    final RecordBatchSizer batchSizer = new RecordBatchSizer(recordBatch);
    final StringBuilder msg = new StringBuilder();

    msg.append(BATCH_STATS_PREFIX);
    msg.append(" Originator: {");
    msg.append(statsId);
    if (sourceId != null) {
      msg.append(':');
      msg.append(sourceId);
    }
    msg.append("}, Batch size: {");
    msg.append( "  Records: " );
    msg.append(batchSizer.rowCount());
    msg.append(", Total size: ");
    msg.append(batchSizer.actualSize());
    msg.append(", Data size: ");
    msg.append(batchSizer.netSize());
    msg.append(", Gross row width: ");
    msg.append(batchSizer.grossRowWidth());
    msg.append(", Net row width: ");
    msg.append(batchSizer.netRowWidth());
    msg.append(", Density: ");
    msg.append(batchSizer.avgDensity());
    msg.append("% }\n");

    if (verbose) {
      msg.append("Batch schema & sizes: {\n");
      for (ColumnSize colSize : batchSizer.columns().values()) {
        msg.append(BATCH_STATS_PREFIX);
        msg.append("\t");
        msg.append(statsId);
        msg.append('\t');
        msg.append(colSize.toString());
        msg.append(" }\n");
      }
      msg.append(" }\n");
    }

    return msg.toString();
  }

  /**
   * Logs record batch statistics for the input record batch (logging happens only
   * when record statistics logging is enabled).
   *
   * @param stats instance identifier
   * @param sourceId optional source identifier for scanners
   * @param recordBatch a set of records
   * @param verbose whether to include fine-grained stats
   * @param logger Logger where to print the record batch statistics
   */
  public static void logRecordBatchStats(String statsId,
    String sourceId,
    RecordBatch recordBatch,
    RecordBatchStatsContext batchStatsLogging,
    org.slf4j.Logger logger) {

    if (!batchStatsLogging.isEnableBatchSzLogging()) {
      return; // NOOP
    }

    final boolean verbose = batchStatsLogging.isEnableFgBatchSzLogging();
    final String msg = printRecordBatchStats(statsId, sourceId, recordBatch, verbose);

    if (batchStatsLogging.useInfoLevelLogging()) {
      logger.info(msg);
    } else {
      logger.debug(msg);
    }
  }

  /**
   * Prints a materialized field type
   * @param field materialized field
   * @param msg string builder where to append the field type
   */
  public static void printFieldType(MaterializedField field, StringBuilder msg) {
    final MajorType type = field.getType();

    msg.append(type.getMinorType().name());
    msg.append(':');
    msg.append(type.getMode().name());
  }

  /**
   * @param allocator dumps allocator statistics
   * @return string with allocator statistics
   */
  public static String printAllocatorStats(BufferAllocator allocator) {
    StringBuilder msg = new StringBuilder();
    msg.append(BATCH_STATS_PREFIX);
    msg.append(": dumping allocator statistics:\n");
    msg.append(BATCH_STATS_PREFIX);
    msg.append(": ");
    msg.append(allocator.toString());

    return msg.toString();
  }

  /**
   * Disabling class object instantiation.
   */
  private RecordBatchStats() {
  }

}