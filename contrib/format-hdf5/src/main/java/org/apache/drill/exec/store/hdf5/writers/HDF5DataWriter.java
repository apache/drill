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

import java.util.ArrayList;
import java.util.List;

import ch.systemsx.cisd.hdf5.IHDF5Reader;

public abstract class HDF5DataWriter {
  protected final IHDF5Reader reader;

  protected final String datapath;

  protected String fieldName;

  protected int colCount;

  protected int counter;

  protected Object[][] compoundData;

  public HDF5DataWriter(IHDF5Reader reader, String datapath) {
    this.reader = reader;
    this.datapath = datapath;
  }

  public HDF5DataWriter(IHDF5Reader reader, String datapath, String fieldName, int colCount) {
    this(reader, datapath);
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

  @SuppressWarnings("unchecked")
  public <T> List<T> getColumn(int columnIndex) {
    List<T> result = new ArrayList<>();
    for (Object[] compoundDatum : compoundData) {
      result.add((T) compoundDatum[columnIndex]);
    }
    return result;
  }

  public abstract int getDataSize();

  public boolean isCompound() {
    return false;
  }
}
