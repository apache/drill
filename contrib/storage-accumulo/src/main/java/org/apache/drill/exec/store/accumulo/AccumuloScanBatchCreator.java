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

import java.util.LinkedList;
import java.util.List;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.ops.ExecutorFragmentContext;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.physical.impl.BatchCreator;
import org.apache.drill.exec.physical.impl.ScanBatch;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.store.RecordReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * BatchCreator for Accumulo scan operations.
 *
 * <p>This class creates the execution pipeline for Accumulo scans by wiring
 * together the AccumuloSubScan with AccumuloRecordReaders.</p>
 *
 * <p>For user impersonation mode, this class creates clients using the
 * delegation token passed from the SubScan. When a delegation token is
 * present, the reader owns the client and is responsible for closing it.</p>
 */
public class AccumuloScanBatchCreator implements BatchCreator<AccumuloSubScan> {
  private static final Logger logger = LoggerFactory.getLogger(AccumuloScanBatchCreator.class);

  @Override
  public ScanBatch getBatch(
      ExecutorFragmentContext context,
      AccumuloSubScan subScan,
      List<RecordBatch> children) throws ExecutionSetupException {

    Preconditions.checkArgument(children.isEmpty(), "AccumuloSubScan should have no children");

    List<RecordReader> readers = new LinkedList<>();
    List<SchemaPath> columns = subScan.getColumns();

    if (columns == null) {
      columns = GroupScan.ALL_COLUMNS;
    }

    try {
      // Determine if we need to create a new client from delegation token
      // or use the shared service client
      AccumuloClient client;
      boolean ownsClient;

      if (subScan.hasDelegationToken()) {
        // User impersonation mode: create a new client from the delegation token
        // The reader will own this client and close it when done
        DelegationTokenInfo tokenInfo = subScan.getDelegationTokenInfo();
        logger.debug("Creating Accumulo client from delegation token for user: {}",
            tokenInfo.getUserName());

        client = subScan.getStoragePlugin().getConnectionManager()
            .createClientWithDelegationToken(tokenInfo);
        ownsClient = true;

        logger.info("Created impersonated Accumulo client for user '{}' to scan table '{}'",
            tokenInfo.getUserName(), subScan.getScanSpec().getTableName());
      } else {
        // Shared user mode: use the service client
        // The reader does not own this client and should not close it
        client = subScan.getStoragePlugin().getClient();
        ownsClient = false;
      }

      // Create a record reader for this sub-scan
      // In the future, we may have multiple readers for different tablet ranges
      AccumuloRecordReader reader = new AccumuloRecordReader(
          client,
          subScan.getScanSpec(),
          columns,
          getMaxRecords(subScan),
          ownsClient);

      readers.add(reader);

    } catch (Exception e) {
      throw new ExecutionSetupException(
          "Failed to create Accumulo record reader for table: " + subScan.getScanSpec().getTableName(), e);
    }

    return new ScanBatch(subScan, context, readers);
  }

  /**
   * Returns the maximum number of records to read, or -1 for unlimited.
   * Uses the more restrictive limit from either the SubScan's maxRecords
   * (set by limit pushdown) or the ScanSpec's limit.
   */
  private int getMaxRecords(AccumuloSubScan subScan) {
    int subScanLimit = subScan.getMaxRecords();
    Integer specLimit = subScan.getScanSpec().getLimit();

    // If both are set, use the smaller one
    if (subScanLimit > 0 && specLimit != null && specLimit > 0) {
      return Math.min(subScanLimit, specLimit);
    }
    // Otherwise, use whichever is set
    if (subScanLimit > 0) {
      return subScanLimit;
    }
    if (specLimit != null && specLimit > 0) {
      return specLimit;
    }
    return -1;
  }
}
