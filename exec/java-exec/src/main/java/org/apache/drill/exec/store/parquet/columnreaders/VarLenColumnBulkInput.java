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

import io.netty.buffer.DrillBuf;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.VarLenBulkEntry;
import org.apache.drill.exec.vector.VarLenBulkInput;
import org.apache.parquet.column.values.ValuesReader;
import org.apache.parquet.io.api.Binary;

/** Implements the {@link VarLenBulkInput} interface to optimize data copy */
final class VarLenColumnBulkInput<V extends ValueVector> implements VarLenBulkInput<VarLenBulkEntry> {
  /** A cut off number for bulk processing */
  private static final int BULK_PROCESSING_MAX_PREC_LEN = 1 << 10;

  /** parent object */
  private final VarLengthValuesColumn<V> parentInst;
  /** Column precision type information (owner by caller) */
  private final ColumnPrecisionInfo columnPrecInfo;
  /** Custom definition level reader */
  private final DefLevelReaderWrapper custDefLevelReader;
  /** Custom dictionary reader */
  private final DictionaryReaderWrapper custDictionaryReader;

  /** The records to read */
  private final int recordsToRead;
  /** Current operation bulk reader state */
  private final OprBulkReadState oprReadState;
  /** Container class for holding page data information */
  private final PageDataInfo pageInfo = new PageDataInfo();
  /** Buffered page payload */
  private VarLenBulkPageReader buffPagePayload;
  /** A callback to allow child readers interact with this class */
  private final VLColumnBulkInputCallback callback;

  /**
   * CTOR.
   * @param parentInst parent object instance
   * @param recordsToRead number of records to read
   * @param columnPrecInfo column precision information
   * @throws IOException runtime exception in case of processing error
   */
  VarLenColumnBulkInput(VarLengthValuesColumn<V> parentInst,
    int recordsToRead, BulkReaderState bulkReaderState) throws IOException {

    this.parentInst = parentInst;
    this.recordsToRead = recordsToRead;
    this.callback = new VLColumnBulkInputCallback(parentInst.pageReader);
    this.columnPrecInfo = bulkReaderState.columnPrecInfo;
    this.custDefLevelReader = bulkReaderState.definitionLevelReader;
    this.custDictionaryReader = bulkReaderState.dictionaryReader;

    // Load page if none have been read
    loadPageIfNeeed();

    // Create the internal READ_STATE object based on the current page-reader state
    this.oprReadState = new OprBulkReadState(parentInst.pageReader.readyToReadPosInBytes, parentInst.pageReader.valuesRead, 0);

    // Let's try to figure out whether this columns is fixed or variable length; this information
    // is not always accurate within the Parquet schema metadata.
    if (ColumnPrecisionType.isPrecTypeUnknown(columnPrecInfo.columnPrecisionType)) {
      guessColumnPrecision(columnPrecInfo);
    }

    // Initialize the buffered-page-payload object
    setBufferedPagePayload();
  }

  /** {@inheritDoc} */
  @Override
  public boolean hasNext() {
    try {
      if (oprReadState.batchFieldIndex < recordsToRead) {
        // We need to ensure there is a page of data to be read
        if (!parentInst.pageReader.hasPage() || parentInst.pageReader.currentPageCount == oprReadState.numPageFieldsProcessed) {
          long totalValueCount = parentInst.columnChunkMetaData.getValueCount();

          if (totalValueCount == (parentInst.totalValuesRead + oprReadState.batchFieldIndex) || !parentInst.pageReader.next()) {
            parentInst.hitRowGroupEnd();
            return false;
          }

          // Reset the state object page read metadata
          oprReadState.numPageFieldsProcessed = 0;
          oprReadState.pageReadPos = parentInst.pageReader.readyToReadPosInBytes;

          // Update the value readers information
          setValuesReadersOnNewPage();

          // Update the buffered-page-payload since we've read a new page
          setBufferedPagePayload();
        }
        return true;
      } else {
        return false;
      }
    } catch (IOException ie) {
      throw new DrillRuntimeException(ie);
    }
  }

  /** {@inheritDoc} */
  @Override
  public final VarLenBulkEntry next() {
    final int toReadRemaining = recordsToRead - oprReadState.batchFieldIndex;
    final int pageRemaining = parentInst.pageReader.currentPageCount - oprReadState.numPageFieldsProcessed;
    final int remaining = Math.min(toReadRemaining, pageRemaining);
    final VarLenBulkEntry result = buffPagePayload.getEntry(remaining);

    // Update position for next read
    if (result != null) {
      // Page read position is meaningful only when dictionary mode is off
      if (pageInfo.dictionaryValueReader == null
       || !pageInfo.dictionaryValueReader.isDefined()) {
        oprReadState.pageReadPos += (result.getTotalLength() + 4 * result.getNumNonNullValues());
      }
      oprReadState.numPageFieldsProcessed += result.getNumValues();
      oprReadState.batchFieldIndex += result.getNumValues();
    }
    return result;
  }

  /** {@inheritDoc} */
  @Override
  public final void remove() {
    throw new UnsupportedOperationException();
  }

  /** {@inheritDoc} */
  @Override
  public final int getStartIndex() {
    return oprReadState.batchFieldIndex;
  }

  /** {@inheritDoc} */
  @Override
  public final void done() {
    // Update the page reader state so that a future call to this method resumes
    // where we left off.

    // Page read position is meaningful only when dictionary mode is off
    if (pageInfo.dictionaryValueReader == null
     || !pageInfo.dictionaryValueReader.isDefined()) {
      parentInst.pageReader.readyToReadPosInBytes = oprReadState.pageReadPos;
    }
    parentInst.pageReader.valuesRead = oprReadState.numPageFieldsProcessed;
    parentInst.totalValuesRead += oprReadState.batchFieldIndex;
  }

  final int getReadBatchFields() {
    return oprReadState.batchFieldIndex;
  }

  private final void setValuesReadersOnNewPage() {
    if (parentInst.pageReader.currentPageCount > 0) {
      custDefLevelReader.set(parentInst.pageReader.definitionLevels, parentInst.pageReader.currentPageCount);
      if (parentInst.usingDictionary) {
        assert parentInst.pageReader.dictionaryValueReader != null : "Dictionary reader should not be null";
        custDictionaryReader.set(parentInst.pageReader.dictionaryValueReader);
      } else {
        custDictionaryReader.set(null);
      }
    } else {
      custDefLevelReader.set(null, 0);
      custDictionaryReader.set(null);
    }
  }

  private final void setBufferedPagePayload() {

    if (parentInst.pageReader.hasPage() && oprReadState.numPageFieldsProcessed < parentInst.pageReader.currentPageCount) {
      if (!parentInst.usingDictionary) {
        pageInfo.pageData  = parentInst.pageReader.pageData;
        pageInfo.pageDataOff = (int) oprReadState.pageReadPos;
        pageInfo.pageDataLen = (int) parentInst.pageReader.byteLength;
      }

      pageInfo.numPageValues = parentInst.pageReader.currentPageCount;
      pageInfo.definitionLevels = custDefLevelReader;
      pageInfo.dictionaryValueReader = custDictionaryReader;
      pageInfo.numPageFieldsRead = oprReadState.numPageFieldsProcessed;

      if (buffPagePayload == null) {
        buffPagePayload = new VarLenBulkPageReader(pageInfo, columnPrecInfo, callback);

      } else {
        buffPagePayload.set(pageInfo);
      }
    } else {
      if (buffPagePayload == null) {
        buffPagePayload = new VarLenBulkPageReader(null, columnPrecInfo, callback);
      }
    }
  }

  final ColumnPrecisionInfo getColumnPrecisionInfo() {
    return columnPrecInfo;
  }

  /** Reads a data sample to evaluate this column's precision (variable or fixed); this is best effort, caller
   * should be ready to handle false positives.
   *
   * @param columnPrecInfo input/output precision info container
   * @throws IOException
   */
  private final void guessColumnPrecision(ColumnPrecisionInfo columnPrecInfo) throws IOException {
    columnPrecInfo.columnPrecisionType = ColumnPrecisionType.DT_PRECISION_IS_VARIABLE;

    loadPageIfNeeed();

    // Minimum number of values within a data size to consider bulk processing
    final int minNumVals = VarLenBulkPageReader.BUFF_SZ / BULK_PROCESSING_MAX_PREC_LEN;
    final int maxDataToProcess =
      Math.min(VarLenBulkPageReader.BUFF_SZ + 4 * minNumVals,
        (int) (parentInst.pageReader.byteLength-parentInst.pageReader.readyToReadPosInBytes));

    if (parentInst.usingDictionary || maxDataToProcess == 0) {
      // The number of values is small, there are lot of null values, or dictionary encoding is used. Bulk
      // processing should work fine for these use-cases
      columnPrecInfo.bulkProcess = true;
      return;
    }

    ByteBuffer buffer = ByteBuffer.allocate(maxDataToProcess);
    buffer.order(ByteOrder.nativeOrder());

    parentInst.pageReader.pageData.getBytes((int) parentInst.pageReader.readyToReadPosInBytes, buffer.array(), 0, maxDataToProcess);
    buffer.limit(maxDataToProcess);

    int numValues = 0;
    int fixedDataLen = -1;
    boolean isFixedPrecision = false;

    do {
      if (buffer.remaining() < 4) {
        break;
      }

      int data_len = buffer.getInt();

      if (fixedDataLen < 0) {
        fixedDataLen = data_len;
        isFixedPrecision = true;
      }

      if (isFixedPrecision && fixedDataLen != data_len) {
        isFixedPrecision = false;
      }

      if (buffer.remaining() < data_len) {
        break;
      }
      buffer.position(buffer.position() + data_len);

      ++numValues;

    } while (true);

    // We need to have encountered at least a couple of values with the same length; if the values
    // have long length, then fixed vs VL is not a big deal with regard to performance.
    if (isFixedPrecision && fixedDataLen >= 0) {
      columnPrecInfo.columnPrecisionType = ColumnPrecisionType.DT_PRECISION_IS_FIXED;
      columnPrecInfo.precision = fixedDataLen;

      if (fixedDataLen <= BULK_PROCESSING_MAX_PREC_LEN) {
        columnPrecInfo.bulkProcess = true;

      } else {
        columnPrecInfo.columnPrecisionType = ColumnPrecisionType.DT_PRECISION_IS_VARIABLE;
        columnPrecInfo.bulkProcess = false;

      }
    } else {
      // At this point we know this column is variable length; we need to figure out whether it is worth
      // processing it in a bulk-manner or not.

      if (numValues >= minNumVals) {
        columnPrecInfo.bulkProcess = true;
      } else {
        columnPrecInfo.bulkProcess = false;
      }
    }
  }

  private void loadPageIfNeeed() throws IOException {
    if (!parentInst.pageReader.hasPage()) {
      // load a page
      parentInst.pageReader.next();
      // update the definition level information
      setValuesReadersOnNewPage();
    }
  }

  // --------------------------------------------------------------------------
  // Inner Classes
  // --------------------------------------------------------------------------

  /** Enumeration which indicates whether a column's type precision is unknown, variable, or fixed. */
  enum ColumnPrecisionType {
    DT_PRECISION_UNKNOWN,
    DT_PRECISION_IS_FIXED,
    DT_PRECISION_IS_VARIABLE;

    static boolean isPrecTypeUnknown(ColumnPrecisionType type) {
      return DT_PRECISION_UNKNOWN.equals(type);
    }

    static boolean isPrecTypeFixed(ColumnPrecisionType type) {
      return DT_PRECISION_IS_FIXED.equals(type);
    }

    static boolean isPrecTypeVariable(ColumnPrecisionType type) {
      return DT_PRECISION_IS_VARIABLE.equals(type);
    }
  }

  /** this class enables us to cache state across bulk reader operations */
  final static class BulkReaderState {

    /** Column Precision Type: variable or fixed length; used to overcome unreliable meta-data information  */
    final ColumnPrecisionInfo columnPrecInfo = new ColumnPrecisionInfo();
    /**
     * A custom definition level reader which overcomes Parquet's ValueReader limitations (that is,
     * no ability to peek)
     */
    final DefLevelReaderWrapper definitionLevelReader = new DefLevelReaderWrapper();
    /**
     * A custom dictionary reader which overcomes Parquet's ValueReader limitations (that is,
     * no ability to peek)
     */
    final DictionaryReaderWrapper dictionaryReader = new DictionaryReaderWrapper();
  }

  /** Container class to hold a column precision information */
  final static class ColumnPrecisionInfo {
    /** column precision type */
    ColumnPrecisionType columnPrecisionType = ColumnPrecisionType.DT_PRECISION_UNKNOWN;
    /** column precision; set only for fixed length precision */
    int precision;
    /** indicator on whether this column should be bulk processed */
    boolean bulkProcess;

    /** Copies source content into this object */
    void clone(final ColumnPrecisionInfo src) {
      columnPrecisionType = src.columnPrecisionType;
      precision = src.precision;
      bulkProcess = src.bulkProcess;
    }

  }

  /** Contains information about current bulk read operation */
  private final static class OprBulkReadState {
    /** reader position within current page */
    long pageReadPos;
    /** number of fields processed within the current page */
    int numPageFieldsProcessed;
    /** field index within current batch */
    int batchFieldIndex;

      OprBulkReadState(long pageReadPos, int numPageFieldsRead, int batchFieldIndex) {
        this.pageReadPos = pageReadPos;
        this.numPageFieldsProcessed = numPageFieldsRead;
        this.batchFieldIndex = batchFieldIndex;
      }
  }

  /** Container class for holding page data information */
  final static class PageDataInfo {
    /** Number of values within the current page */
    int numPageValues;
    /** Page data buffer */
    DrillBuf pageData;
    /** Offset within the page data */
    int pageDataOff;
    /** Page data length */
    int pageDataLen;
    /** number of fields read within current page */
    int numPageFieldsRead;
    /** Definition Level */
    DefLevelReaderWrapper definitionLevels;
    /** Dictionary value reader */
    DictionaryReaderWrapper dictionaryValueReader;
  }

  /** Callback to allow a bulk reader interact with its parent */
  final static class VLColumnBulkInputCallback {
    /** Page reader object */
    PageReader pageReader;

    VLColumnBulkInputCallback(PageReader _pageReader) {
      this.pageReader = _pageReader;
    }

    /**
     * Enables Parquet column readers to reset the definition level reader to a specific state.
     * @param skipCount the number of rows to skip (optional)
     *
     * @throws IOException An IO related condition
     */
    void resetDefinitionLevelReader(int skipCount) throws IOException {
      pageReader.resetDefinitionLevelReader(skipCount);
    }

    /**
     * @return current page definition level
     */
    ValuesReader getDefinitionLevelsReader() {
      return pageReader.definitionLevels;
    }
  }

  /** A wrapper value reader with the ability to control when to read the next value */
  final static class DefLevelReaderWrapper {
    /** Definition Level */
    private ValuesReader definitionLevels;
    /** Peeked value     */
    private int currValue;
    /** Remaining values */
    private int remaining;

    /**
     * @return true if the current page has definition levels to be read
     */
    public boolean hasDefinitionLevels() {
      return definitionLevels != null;
    }

    /**
     * Consume the first integer if not done; we want to empower the caller so to avoid extra checks
     * during access methods (e.g., some consumers will not invoke this method as they rather access
     * the raw reader..)
     */
    public void readFirstIntegerIfNeeded() {
      assert definitionLevels != null;
      if (currValue == -1) {
        setNextInteger();
      }
    }

    /**
     * Set the {@link PageReader#definitionLevels} object; if a null value is passed, then it is understood
     * the current page doesn't have definition levels to be processed
     * @param definitionLevels {@link ValuesReader} object
     * @param numValues total number of values that can be read from the stream
     */
    void set(ValuesReader definitionLevels, int numValues) {
      this.definitionLevels = definitionLevels;
      this.currValue = -1;
      this.remaining = numValues;
    }

    /**
     * @return the current integer from the page; this method has no side-effects (the underlying
     *         {@link ValuesReader} is not affected)
     */
    public int readCurrInteger() {
      assert currValue >= 0;
      return currValue;
    }

    /**
     * @return internally reads the next integer from the underlying {@link ValuesReader}; false if the stream
     *         reached EOF
     */
    public boolean nextIntegerIfNotEOF() {
      return setNextInteger();
    }

    /**
     * @return underlying reader object; this object is now unusable
     *         note that you have to invoke the {@link #set(ValuesReader, int)} method
     *         to update this object state in case a) you have used the {@link ValuesReader} object and b)
     *         want to resume using this {@link DefinitionLevelReader} object instance
     */
    public ValuesReader getUnderlyingReader() {
      currValue = -1; // to make this object unusable
      return definitionLevels;
    }

    private boolean setNextInteger() {
      if (remaining > 0) {
        --remaining;
        try {
          currValue = definitionLevels.readInteger();
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
        return true;
      }
      currValue = -1;
      return false;
    }
  }

  /** A wrapper value reader with the ability to control when to read the next value */
  final static class DictionaryReaderWrapper {
    /** Dictionary Reader */
    private ValuesReader valuesReader;
    /** Pushed back value     */
    private Binary pushedBackValue;

    /**
     * @return true if the current page uses dictionary encoding for the data
     */
    public boolean isDefined() {
      return valuesReader != null;
    }

    /**
     * Set the {@link PageReader#dictionaryValueReader} object; if a null value is passed, then it is understood
     * the current page doesn't use dictionary encoding
     * @param valuesReader {@link ValuesReader} object
     * @param numValues total number of values that can be read from the stream
     */
    void set(ValuesReader _rawReader) {
      this.valuesReader    = _rawReader;
      this.pushedBackValue = null;
    }

    /**
     * @return the current entry from the page
     */
    public Binary getEntry() {
      Binary entry = null;
      if (pushedBackValue == null) {
        entry = getNextEntry();

      } else {
        entry           = pushedBackValue;
        pushedBackValue = null;
      }
      return entry;
    }

    public void pushBack(Binary entry) {
      pushedBackValue = entry;
    }

    private Binary getNextEntry() {
      try {
        return valuesReader.readBytes();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }


}