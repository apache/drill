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
package org.apache.drill.exec.store.parquet.columnreaders;

import java.nio.ByteBuffer;
import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.exec.store.parquet.columnreaders.VarLenColumnBulkInput.ColumnPrecisionInfo;
import org.apache.drill.exec.store.parquet.columnreaders.VarLenColumnBulkInput.PageDataInfo;

/** Handles variable data types. */
final class VarLenEntryReader extends VarLenAbstractEntryReader {

  VarLenEntryReader(ByteBuffer buffer,
    PageDataInfo pageInfo,
    ColumnPrecisionInfo columnPrecInfo,
    VarLenColumnBulkEntry entry) {

    super(buffer, pageInfo, columnPrecInfo, entry);
  }

  /** {@inheritDoc} */
  @Override
  VarLenColumnBulkEntry getEntry(int valuesToRead) {
    // Bulk processing is effecting for smaller precisions
    if (bulkProcess()) {
      return getEntryBulk(valuesToRead);
    }
    return getEntrySingle(valuesToRead);
  }

  private final VarLenColumnBulkEntry getEntryBulk(int valuesToRead) {

    load(true); // load new data to process

    final int[] valueLengths = entry.getValuesLength();
    final int readBatch = Math.min(entry.getMaxEntries(), valuesToRead);
    final byte[] tgtBuff = entry.getInternalDataArray();
    final byte[] srcBuff = buffer.array();
    final int srcLen = buffer.remaining();
    final int tgtLen = tgtBuff.length;

    // Counters
    int numValues = 0;
    int tgtPos = 0;
    int srcPos = 0;

    for (; numValues < readBatch; ) {
      if (srcPos > srcLen -4) {
        break;
      }

      final int data_len = getInt(srcBuff, srcPos);
      srcPos += 4;

      if (srcLen < (srcPos + data_len)
       || tgtLen < (tgtPos + data_len)) {

        break;
      }

      valueLengths[numValues++] = data_len;

      if (data_len > 0) {
        vlCopy(srcBuff, srcPos, tgtBuff, tgtPos, data_len);

        // Update the counters
        srcPos += data_len;
        tgtPos += data_len;
      }
    }

    // We're here either because a) the Parquet metadata is wrong (advertises more values than the real count)
    // or the first value being processed ended up to be too long for the buffer.
    if (numValues == 0) {
      return getEntrySingle(valuesToRead);
    }

    // Update the page data buffer offset
    pageInfo.pageDataOff += (numValues * 4 + tgtPos);

    if (remainingPageData() < 0) {
      final String message = String.format("Invalid Parquet page data offset [%d]..", pageInfo.pageDataOff);
      throw new DrillRuntimeException(message);
    }

    // Now set the bulk entry
    entry.set(0, tgtPos, numValues, numValues);

    return entry;
  }

  private final VarLenColumnBulkEntry getEntrySingle(int valuesToRead) {

    if (remainingPageData() < 4) {
      final String message = String.format("Invalid Parquet page metadata; cannot process advertised page count..");
      throw new DrillRuntimeException(message);
    }

    final int[] valueLengths = entry.getValuesLength();
    final int dataLen = pageInfo.pageData.getInt(pageInfo.pageDataOff);

    if (remainingPageData() < (4 + dataLen)) {
      final String message = String.format("Invalid Parquet page metadata; cannot process advertised page count..");
      throw new DrillRuntimeException(message);
    }

    // Register the length
    valueLengths[0] = dataLen;

    // Now set the bulk entry
    entry.set(pageInfo.pageDataOff + 4, dataLen, 1, 1, pageInfo.pageData);

    // Update the page data buffer offset
    pageInfo.pageDataOff += (dataLen + 4);

    return entry;
  }

}
