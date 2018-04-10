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
package org.apache.drill.test.rowSet;

import java.util.List;

import org.apache.drill.exec.physical.rowSet.model.ReaderIndex;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.vector.accessor.reader.AbstractObjectReader;
import org.apache.drill.exec.vector.accessor.reader.AbstractTupleReader;

/**
 * Reader implementation for a row set.
 */

public class RowSetReaderImpl extends AbstractTupleReader implements RowSetReader {

  protected final ReaderIndex readerIndex;

  public RowSetReaderImpl(TupleMetadata schema, ReaderIndex index, AbstractObjectReader[] readers) {
    super(schema, readers);
    this.readerIndex = index;
    bindIndex(index);
  }

  public RowSetReaderImpl(TupleMetadata schema, ReaderIndex index,
      List<AbstractObjectReader> readers) {
    this(schema, index,
        readers.toArray(new AbstractObjectReader[readers.size()]));
  }

  @Override
  public boolean next() {
    if (! readerIndex.next()) {
      return false;
    }
    reposition();
    return true;
  }

  @Override
  public boolean valid() { return readerIndex.valid(); }

  @Override
  public int index() { return readerIndex.position(); }

  @Override
  public int rowCount() { return readerIndex.size(); }

  @Override
  public int rowIndex() { return readerIndex.vectorIndex(); }

  @Override
  public int batchIndex() { return readerIndex.batchIndex(); }

  @Override
  public void set(int index) {
    this.readerIndex.set(index);
    reposition();
  }
}
