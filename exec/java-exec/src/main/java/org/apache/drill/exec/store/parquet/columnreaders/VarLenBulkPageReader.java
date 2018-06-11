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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.exec.store.parquet.columnreaders.VarLenColumnBulkInput.ColumnPrecisionInfo;
import org.apache.drill.exec.store.parquet.columnreaders.VarLenColumnBulkInput.ColumnPrecisionType;
import org.apache.drill.exec.store.parquet.columnreaders.VarLenColumnBulkInput.PageDataInfo;
import org.apache.drill.exec.store.parquet.columnreaders.VarLenColumnBulkInput.VLColumnBulkInputCallback;

/** Provides bulk reads when accessing Parquet's page payload for variable length columns */
final class VarLenBulkPageReader {

  /**
   * Using small buffers so that they could fit in the L1 cache
   * NOTE - This buffer size is used in several places of the bulk processing implementation; please analyze
   *        the impact of changing this buffer size.
   */
  static final int BUFF_SZ = 1 << 12; // 4k
  static final int PADDING = 1 << 6; // 128bytes padding to allow for access optimizations

  /** byte buffer used for buffering page data */
  private final ByteBuffer buffer = ByteBuffer.allocate(BUFF_SZ + PADDING);
  /** Page Data Information */
  private final PageDataInfo pageInfo = new PageDataInfo();
  /** expected precision type: fixed or variable length */
  private final ColumnPrecisionInfo columnPrecInfo;
  /** Bulk entry */
  private final VarLenColumnBulkEntry entry;
  /** A callback to allow bulk readers interact with their container */
  private final VLColumnBulkInputCallback containerCallback;

  // Various BulkEntry readers
  final VarLenAbstractEntryReader fixedReader;
  final VarLenAbstractEntryReader nullableFixedReader;
  final VarLenAbstractEntryReader variableLengthReader;
  final VarLenAbstractEntryReader nullableVLReader;
  final VarLenAbstractEntryReader dictionaryReader;
  final VarLenAbstractEntryReader nullableDictionaryReader;

  VarLenBulkPageReader(
    PageDataInfo pageInfoInput,
    ColumnPrecisionInfo columnPrecInfoInput,
    VLColumnBulkInputCallback containerCallbackInput) {

    // Set the buffer to the native byte order
    this.buffer.order(ByteOrder.nativeOrder());

    if (pageInfoInput != null) {
      this.pageInfo.pageData = pageInfoInput.pageData;
      this.pageInfo.pageDataOff = pageInfoInput.pageDataOff;
      this.pageInfo.pageDataLen = pageInfoInput.pageDataLen;
      this.pageInfo.numPageFieldsRead = pageInfoInput.numPageFieldsRead;
      this.pageInfo.definitionLevels = pageInfoInput.definitionLevels;
      this.pageInfo.dictionaryValueReader = pageInfoInput.dictionaryValueReader;
      this.pageInfo.numPageValues = pageInfoInput.numPageValues;
    }

    this.columnPrecInfo = columnPrecInfoInput;
    this.entry = new VarLenColumnBulkEntry(this.columnPrecInfo);
    this.containerCallback = containerCallbackInput;

    // Initialize the Variable Length Entry Readers
    fixedReader = new VarLenFixedEntryReader(buffer, pageInfo, columnPrecInfo, entry);
    nullableFixedReader = new VarLenNullableFixedEntryReader(buffer, pageInfo, columnPrecInfo, entry);
    variableLengthReader = new VarLenEntryReader(buffer, pageInfo, columnPrecInfo, entry);
    nullableVLReader = new VarLenNullableEntryReader(buffer, pageInfo, columnPrecInfo, entry);
    dictionaryReader = new VarLenEntryDictionaryReader(buffer, pageInfo, columnPrecInfo, entry);
    nullableDictionaryReader = new VarLenNullableDictionaryReader(buffer, pageInfo, columnPrecInfo, entry);
  }

  final void set(PageDataInfo pageInfoInput) {
    pageInfo.pageData = pageInfoInput.pageData;
    pageInfo.pageDataOff = pageInfoInput.pageDataOff;
    pageInfo.pageDataLen = pageInfoInput.pageDataLen;
    pageInfo.numPageFieldsRead = pageInfoInput.numPageFieldsRead;
    pageInfo.definitionLevels = pageInfoInput.definitionLevels;
    pageInfo.dictionaryValueReader = pageInfoInput.dictionaryValueReader;
    pageInfo.numPageValues = pageInfoInput.numPageValues;

    buffer.clear();
  }

  final VarLenColumnBulkEntry getEntry(int valuesToRead) {
    VarLenColumnBulkEntry entry = null;

    if (ColumnPrecisionType.isPrecTypeFixed(columnPrecInfo.columnPrecisionType)) {
      if ((entry = getFixedEntry(valuesToRead)) == null) {
        // The only reason for a null to be returned is when the "getFixedEntry" method discovers
        // the column is not fixed length; this false positive happens if the sample data was not
        // representative of all the column values.

        // If this is an optional column, then we need to reset the definition-level reader
        if (pageInfo.definitionLevels.hasDefinitionLevels()) {
          try {
            containerCallback.resetDefinitionLevelReader(pageInfo.numPageFieldsRead);
            // Update the definition level object reference
            pageInfo.definitionLevels.set(containerCallback.getDefinitionLevelsReader(),
              pageInfo.numPageValues - pageInfo.numPageFieldsRead);

          } catch (IOException ie) {
            throw new DrillRuntimeException(ie);
          }
        }

        columnPrecInfo.columnPrecisionType = ColumnPrecisionType.DT_PRECISION_IS_VARIABLE;
        entry = getVLEntry(valuesToRead);
      }

    } else {
      entry = getVLEntry(valuesToRead);
    }

    if (entry != null) {
      pageInfo.numPageFieldsRead += entry.getNumValues();
    }
    return entry;
  }

  private final VarLenColumnBulkEntry getFixedEntry(int valuesToRead) {
    if (pageInfo.definitionLevels.hasDefinitionLevels()) {
      return nullableFixedReader.getEntry(valuesToRead);
    } else {
      return fixedReader.getEntry(valuesToRead);
    }
  }

  private final VarLenColumnBulkEntry getVLEntry(int valuesToRead) {
    if (pageInfo.dictionaryValueReader == null
     || !pageInfo.dictionaryValueReader.isDefined()) {

      if (pageInfo.definitionLevels.hasDefinitionLevels()) {
        return nullableVLReader.getEntry(valuesToRead);
      } else {
        return variableLengthReader.getEntry(valuesToRead);
      }
    } else {
      if (pageInfo.definitionLevels.hasDefinitionLevels()) {
        return nullableDictionaryReader.getEntry(valuesToRead);
      } else {
        return dictionaryReader.getEntry(valuesToRead);
      }
    }
  }

}