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
package org.apache.drill.exec.store.easy.text.reader;


/**
 * Base class for producing output record batches while dealing with
 * text files. Defines the interface called from text parsers to create
 * the corresponding value vectors (record batch).
 */

abstract class TextOutput {

  public abstract void startRecord();

  /**
   * Start processing a new field within a record.
   * @param index  index within the record
   */
  public abstract void startField(int index);

  /**
   * End processing a field within a record.
   * @return  true if engine should continue processing record.  false if rest of record can be skipped.
   */
  public abstract boolean endField();

  /**
   * Shortcut that lets the output know that we are closing ending a field with no data.
   * @return true if engine should continue processing record.  false if rest of record can be skipped.
   */
  public abstract boolean endEmptyField();

  /**
   * Add the provided data but drop any whitespace.
   * @param data character to append
   */
  public void appendIgnoringWhitespace(byte data) {
    if (TextReader.isWhite(data)) {
      // noop
    } else {
      append(data);
    }
  }

  /**
   * Appends a byte to the output character data buffer
   * @param data  current byte read
   */
  public abstract void append(byte data);

  /**
   * Completes the processing of a given record. Also completes the processing of the
   * last field being read.
   */
  public abstract void finishRecord();

  /**
   *  Return the total number of records (across batches) processed
   */
  public abstract long getRecordCount();

  /**
   * Indicates if the current batch is full and reading for this batch
   * should stop.
   *
   * @return true if the batch is full and the reader must exit to allow
   * the batch to be sent downstream, false if the reader may continue to
   * add rows to the current batch
   */

  public abstract boolean isFull();
}
