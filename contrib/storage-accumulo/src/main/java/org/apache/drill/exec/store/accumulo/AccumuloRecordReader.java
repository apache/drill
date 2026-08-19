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
package org.apache.drill.exec.store.accumulo;

import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.PathSegment;
import org.apache.drill.common.expression.PathSegment.NameSegment;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.ops.OperatorStats;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.store.AbstractRecordReader;
import org.apache.drill.exec.vector.NullableVarBinaryVector;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.VarBinaryVector;
import org.apache.drill.exec.vector.complex.MapVector;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Sets;

/**
 * RecordReader for Accumulo storage plugin.
 *
 * <p>This reader scans Accumulo tables and populates Drill value vectors.
 * It uses the dynamic schema approach similar to HBase, where column families
 * are represented as maps containing their qualifiers as fields.</p>
 *
 * <p>Row structure:</p>
 * <ul>
 *   <li>row_key: VARBINARY - the Accumulo row key</li>
 *   <li>Each column family becomes a MAP with qualifier names as keys</li>
 * </ul>
 *
 * <p>For user impersonation mode, the reader may own the AccumuloClient
 * (created from a delegation token) and is responsible for closing it.
 * For shared user mode, the reader uses a shared client and should not close it.</p>
 */
public class AccumuloRecordReader extends AbstractRecordReader implements DrillAccumuloConstants {
  private static final Logger logger = LoggerFactory.getLogger(AccumuloRecordReader.class);

  // Batch constraints to avoid OOM
  private static final int MAX_ALLOCATED_MEMORY_PER_BATCH = 64 * 1024 * 1024; // 64 MB
  private static final int TARGET_RECORD_COUNT = DEFAULT_BATCH_SIZE;

  private final AccumuloClient client;
  private final AccumuloScanSpec scanSpec;
  private final int maxRecords;

  /**
   * Whether this reader owns the client and should close it.
   * True for user impersonation mode (client created from delegation token),
   * false for shared user mode (client is shared/pooled).
   */
  private final boolean ownsClient;

  private OutputMutator outputMutator;
  private OperatorContext operatorContext;

  private Scanner scanner;
  private Iterator<Map.Entry<Key, Value>> scanIterator;

  private Map<String, MapVector> familyVectorMap;
  private VarBinaryVector rowKeyVector;

  private Set<String> requestedFamilies;
  private Map<String, Set<String>> requestedColumns;  // family -> set of qualifiers
  private boolean rowKeyOnly;
  private int recordsRead;

  /**
   * Creates an AccumuloRecordReader with a shared client (does not own the client).
   */
  public AccumuloRecordReader(
      AccumuloClient client,
      AccumuloScanSpec scanSpec,
      List<SchemaPath> projectedColumns,
      int maxRecords) {
    this(client, scanSpec, projectedColumns, maxRecords, false);
  }

  /**
   * Creates an AccumuloRecordReader with explicit client ownership.
   *
   * @param client the Accumulo client to use
   * @param scanSpec the scan specification
   * @param projectedColumns columns to project
   * @param maxRecords maximum records to read (-1 for unlimited)
   * @param ownsClient true if this reader owns the client and should close it
   */
  public AccumuloRecordReader(
      AccumuloClient client,
      AccumuloScanSpec scanSpec,
      List<SchemaPath> projectedColumns,
      int maxRecords,
      boolean ownsClient) {
    this.client = Preconditions.checkNotNull(client, "AccumuloClient required");
    this.scanSpec = Preconditions.checkNotNull(scanSpec, "AccumuloScanSpec required");
    this.maxRecords = maxRecords > 0 ? maxRecords : Integer.MAX_VALUE;
    this.recordsRead = 0;
    this.ownsClient = ownsClient;

    setColumns(projectedColumns);

    if (ownsClient) {
      logger.debug("RecordReader owns the AccumuloClient and will close it when done");
    }
  }

  /**
   * Transforms projected columns and determines which Accumulo columns to fetch.
   */
  @Override
  protected Collection<SchemaPath> transformColumns(Collection<SchemaPath> columns) {
    Set<SchemaPath> transformed = Sets.newLinkedHashSet();
    requestedFamilies = Sets.newHashSet();
    requestedColumns = new HashMap<>();

    rowKeyOnly = true;

    if (!isStarQuery()) {
      for (SchemaPath column : columns) {
        if (column.getRootSegment().getPath().equalsIgnoreCase(ROW_KEY)) {
          transformed.add(ROW_KEY_PATH);
          continue;
        }

        rowKeyOnly = false;
        NameSegment root = column.getRootSegment();
        String family = root.getPath();
        transformed.add(SchemaPath.getSimplePath(family));

        PathSegment child = root.getChild();
        if (child != null && child.isNamed()) {
          // Specific column within family: cf.qualifier
          String qualifier = child.getNameSegment().getPath();
          requestedColumns.computeIfAbsent(family, k -> Sets.newHashSet()).add(qualifier);
        } else {
          // Entire column family requested
          requestedFamilies.add(family);
        }
      }
    } else {
      rowKeyOnly = false;
      transformed.add(ROW_KEY_PATH);
    }

    return transformed;
  }

  @Override
  public void setup(OperatorContext context, OutputMutator output) throws ExecutionSetupException {
    this.operatorContext = context;
    this.outputMutator = output;
    familyVectorMap = new HashMap<>();

    try {
      // Create scanner
      scanner = client.createScanner(scanSpec.getTableName(), Authorizations.EMPTY);

      // Configure scan range
      configureRange();

      // Configure which columns to fetch
      configureColumns();

      // Set batch size
      scanner.setBatchSize(TARGET_RECORD_COUNT);

      // Setup output vectors
      setupOutputVectors();

      // Get iterator
      scanIterator = scanner.iterator();

    } catch (TableNotFoundException e) {
      throw new ExecutionSetupException("Accumulo table not found: " + scanSpec.getTableName(), e);
    } catch (SchemaChangeException e) {
      throw new ExecutionSetupException("Schema setup failed", e);
    }
  }

  /**
   * Configures the scan range based on start/stop rows.
   */
  private void configureRange() {
    byte[] startRow = scanSpec.getStartRow();
    byte[] stopRow = scanSpec.getStopRow();

    if (startRow == null && stopRow == null) {
      // Full table scan (default)
      return;
    }

    // The inclusive flags carry the difference between, say, `row_key < 'x'` and
    // `row_key <= 'x'`, so they have to be passed through rather than assumed. A null
    // bound means unbounded on that side.
    scanner.setRange(new Range(
        startRow == null ? null : new Text(startRow), scanSpec.isStartRowInclusive(),
        stopRow == null ? null : new Text(stopRow), scanSpec.isStopRowInclusive()));
  }

  /**
   * Configures which column families/qualifiers to fetch.
   */
  private void configureColumns() {
    // If specific columns requested from the scan spec, use those
    List<AccumuloScanSpec.AccumuloColumnSpec> specColumns = scanSpec.getColumns();
    if (specColumns != null && !specColumns.isEmpty()) {
      for (AccumuloScanSpec.AccumuloColumnSpec col : specColumns) {
        String family = col.getColumnFamily();
        String qualifier = col.getColumnQualifier();
        if (qualifier != null && !qualifier.isEmpty()) {
          scanner.fetchColumn(new Text(family), new Text(qualifier));
        } else {
          scanner.fetchColumnFamily(new Text(family));
        }
      }
      return;
    }

    // Otherwise use the projected columns
    if (rowKeyOnly || isStarQuery()) {
      // Fetch all columns
      return;
    }

    // Fetch entire requested families
    for (String family : requestedFamilies) {
      scanner.fetchColumnFamily(new Text(family));
    }

    // Fetch specific columns (but only if their family isn't already fully requested)
    for (Map.Entry<String, Set<String>> entry : requestedColumns.entrySet()) {
      String family = entry.getKey();
      if (!requestedFamilies.contains(family)) {
        for (String qualifier : entry.getValue()) {
          scanner.fetchColumn(new Text(family), new Text(qualifier));
        }
      }
    }
  }

  /**
   * Sets up output vectors based on requested columns.
   */
  private void setupOutputVectors() throws SchemaChangeException {
    // Add row_key vector
    for (SchemaPath column : getColumns()) {
      if (column.equals(ROW_KEY_PATH)) {
        MaterializedField field = MaterializedField.create(ROW_KEY, ROW_KEY_TYPE);
        rowKeyVector = outputMutator.addField(field, VarBinaryVector.class);
      } else {
        getOrCreateFamilyVector(column.getRootSegment().getPath(), false);
      }
    }
  }

  @Override
  public int next() {
    Stopwatch watch = Stopwatch.createStarted();

    // Clear and allocate vectors
    if (rowKeyVector != null) {
      rowKeyVector.clear();
      rowKeyVector.allocateNew();
    }
    for (ValueVector v : familyVectorMap.values()) {
      v.clear();
      v.allocateNew();
    }

    int rowCount = 0;
    String currentRowKey = null;
    int currentRowIndex = -1;

    OperatorStats operatorStats = operatorContext == null ? null : operatorContext.getStats();

    while (canAddNewRow(rowCount) && recordsRead < maxRecords) {
      Map.Entry<Key, Value> entry = null;

      try {
        if (operatorStats != null) {
          operatorStats.startWait();
        }
        try {
          if (!scanIterator.hasNext()) {
            break;
          }
          entry = scanIterator.next();
        } finally {
          if (operatorStats != null) {
            operatorStats.stopWait();
          }
        }
      } catch (Exception e) {
        throw new DrillRuntimeException("Error reading from Accumulo", e);
      }

      Key key = entry.getKey();
      byte[] rowKeyBytes = key.getRow().getBytes();
      String rowKeyStr = new String(rowKeyBytes, StandardCharsets.UTF_8);

      // Check if this is a new row
      if (!rowKeyStr.equals(currentRowKey)) {
        if (currentRowKey != null) {
          // Finished previous row, increment row count
          rowCount++;
          recordsRead++;
        }

        // Check limits again after incrementing
        if (!canAddNewRow(rowCount) || recordsRead >= maxRecords) {
          // Can't add more rows, but we consumed this entry
          // We need to handle this edge case - for now we'll include it
          if (rowCount >= TARGET_RECORD_COUNT || recordsRead >= maxRecords) {
            break;
          }
        }

        currentRowKey = rowKeyStr;
        currentRowIndex = rowCount;

        // Set row key
        if (rowKeyVector != null) {
          rowKeyVector.getMutator().setSafe(currentRowIndex, rowKeyBytes, 0, rowKeyBytes.length);
        }
      }

      // Skip value population if row_key only query
      if (!rowKeyOnly) {
        String family = key.getColumnFamily().toString();
        String qualifier = key.getColumnQualifier().toString();
        byte[] valueBytes = entry.getValue().get();

        MapVector familyVector = getOrCreateFamilyVector(family, true);
        NullableVarBinaryVector qualifierVector = getOrCreateColumnVector(familyVector, qualifier);
        qualifierVector.getMutator().setSafe(currentRowIndex, valueBytes, 0, valueBytes.length);
      }
    }

    // Don't forget the last row
    if (currentRowKey != null && currentRowIndex == rowCount) {
      rowCount++;
      recordsRead++;
    }

    setOutputRowCount(rowCount);

    logger.debug("Read {} records from {} in {} ms",
        rowCount, scanSpec.getTableName(), watch.elapsed(TimeUnit.MILLISECONDS));

    return rowCount;
  }

  /**
   * Gets or creates a MapVector for the given column family.
   */
  private MapVector getOrCreateFamilyVector(String familyName, boolean allocateOnCreate) {
    try {
      MapVector v = familyVectorMap.get(familyName);
      if (v == null) {
        SchemaPath column = SchemaPath.getSimplePath(familyName);
        MaterializedField field = MaterializedField.create(column.getAsNamePart().getName(), COLUMN_FAMILY_TYPE);
        v = outputMutator.addField(field, MapVector.class);
        if (allocateOnCreate) {
          v.allocateNew();
        }
        getColumns().add(column);
        familyVectorMap.put(familyName, v);
      }
      return v;
    } catch (SchemaChangeException e) {
      throw new DrillRuntimeException(e);
    }
  }

  /**
   * Gets or creates a column vector within a family MapVector.
   */
  private NullableVarBinaryVector getOrCreateColumnVector(MapVector mv, String qualifier) {
    int oldSize = mv.size();
    NullableVarBinaryVector v = mv.addOrGet(qualifier, COLUMN_TYPE, NullableVarBinaryVector.class);
    if (oldSize != mv.size()) {
      v.allocateNew();
    }
    return v;
  }

  /**
   * Sets the value count on all output vectors.
   */
  private void setOutputRowCount(int count) {
    for (ValueVector vv : familyVectorMap.values()) {
      vv.getMutator().setValueCount(count);
    }
    if (rowKeyVector != null) {
      rowKeyVector.getMutator().setValueCount(count);
    }
  }

  /**
   * Checks if a new row can be added to the current batch.
   */
  private boolean canAddNewRow(int rowCount) {
    return rowCount < TARGET_RECORD_COUNT &&
        operatorContext.getAllocator().getAllocatedMemory() < MAX_ALLOCATED_MEMORY_PER_BATCH;
  }

  @Override
  public void close() throws Exception {
    // Close the scanner
    if (scanner != null) {
      try {
        scanner.close();
      } catch (Exception e) {
        logger.warn("Error closing Accumulo scanner for table {}", scanSpec.getTableName(), e);
      }
    }

    // Close the client only if we own it (user impersonation mode)
    if (ownsClient && client != null) {
      try {
        logger.debug("Closing owned AccumuloClient for table {}", scanSpec.getTableName());
        client.close();
      } catch (Exception e) {
        logger.warn("Error closing Accumulo client for table {}", scanSpec.getTableName(), e);
      }
    }
  }

  @Override
  public String toString() {
    return "AccumuloRecordReader[table=" + scanSpec.getTableName() + ", ownsClient=" + ownsClient + "]";
  }
}
