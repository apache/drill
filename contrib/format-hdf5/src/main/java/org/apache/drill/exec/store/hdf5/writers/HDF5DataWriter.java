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

package org.apache.drill.exec.store.hdf5.writers;

import ch.systemsx.cisd.hdf5.IHDF5Reader;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.exec.physical.resultSet.RowSetLoader;
import org.apache.drill.exec.record.metadata.ColumnMetadata;
import org.apache.drill.exec.record.metadata.MetadataUtils;
import org.apache.drill.exec.vector.accessor.ScalarWriter;
import org.apache.drill.exec.vector.accessor.TupleWriter;

import java.util.ArrayList;
import java.util.List;

public abstract class HDF5DataWriter {
  protected final RowSetLoader columnWriter;

  protected final IHDF5Reader reader;

  protected final String datapath;

  protected String fieldName;

  protected int colCount;

  protected int counter;

  protected Object[][] compoundData;

  public HDF5DataWriter(IHDF5Reader reader, RowSetLoader columnWriter, String datapath) {
    this.reader = reader;
    this.columnWriter = columnWriter;
    this.datapath = datapath;
  }

  public HDF5DataWriter(IHDF5Reader reader, RowSetLoader columnWriter, String datapath, String fieldName, int colCount) {
    this.reader = reader;
    this.columnWriter = columnWriter;
    this.datapath = datapath;
    this.fieldName = fieldName;
    this.colCount = colCount;
  }

  public boolean write() {
    return false;
  }

  public boolean hasNext() {
    return false;
  }

  public int currentRowCount() {
    return counter;
  }

  public List<Object> getColumn(int columnIndex) {
    List<Object> result = new ArrayList<>();
    for (Object[] compoundDatum : compoundData) {
      result.add(compoundDatum[columnIndex]);
    }
    return result;
  }

  public abstract int getDataSize();

  public boolean isCompound() {
    return false;
  }

  protected static ScalarWriter makeWriter(TupleWriter tupleWriter, String name, TypeProtos.MinorType type, TypeProtos.DataMode mode) {
    ColumnMetadata colSchema = MetadataUtils.newScalar(name, type, mode);
    int index = tupleWriter.addColumn(colSchema);
    return tupleWriter.scalar(index);
  }

}
