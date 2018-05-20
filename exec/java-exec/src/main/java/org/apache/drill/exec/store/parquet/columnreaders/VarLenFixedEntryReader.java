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
import org.apache.drill.exec.store.parquet.columnreaders.VarLenColumnBulkInput.ColumnPrecisionInfo;
import org.apache.drill.exec.store.parquet.columnreaders.VarLenColumnBulkInput.PageDataInfo;

/** Handles fixed data types that have been erroneously tagged as Variable Length. */
final class VarLenFixedEntryReader extends VarLenAbstractEntryReader {

  VarLenFixedEntryReader(ByteBuffer buffer,
    PageDataInfo pageInfo,
    ColumnPrecisionInfo columnPrecInfo,
    VarLenColumnBulkEntry entry) {

    super(buffer, pageInfo, columnPrecInfo, entry);
  }

  /** {@inheritDoc} */
  @Override
  final VarLenColumnBulkEntry getEntry(int valuesToRead) {
    assert columnPrecInfo.precision >= 0 : "Fixed length precision cannot be lower than zero";

    load(true); // load new data to process

    final int expectedDataLen = columnPrecInfo.precision;
    final int entrySz = 4 + columnPrecInfo.precision;
    final int maxValues = Math.min(entry.getMaxEntries(), (pageInfo.pageDataLen - pageInfo.pageDataOff) / entrySz);
    final int readBatch = Math.min(maxValues, valuesToRead);
    final int[] valueLengths = entry.getValuesLength();
    final byte[] tgtBuff = entry.getInternalDataArray();
    final byte[] srcBuff = buffer.array();
    int idx = 0;

    for ( ; idx < readBatch; ++idx) {
      final int currPos = idx * entrySz;
      final int dataLen = getInt(srcBuff, currPos);

      if (dataLen != expectedDataLen) {
        return null; // this is a soft error; caller needs to revert to variable length processing
      }

      valueLengths[idx] = dataLen;
      final int tgt_pos = idx * expectedDataLen;

      if (expectedDataLen > 0) {
        vlCopy(srcBuff, currPos + 4, tgtBuff, tgt_pos, dataLen);
      }
    }

    // Update the page data buffer offset
    pageInfo.pageDataOff += idx * entrySz;

    // Now set the bulk entry
    entry.set(0, idx * expectedDataLen, idx, idx);

    return entry;
  }
}
