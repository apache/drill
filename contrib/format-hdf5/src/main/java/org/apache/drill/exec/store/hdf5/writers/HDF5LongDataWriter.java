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

import java.util.List;

import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.exec.store.hdf5.HDF5Utils;
import org.apache.drill.exec.vector.accessor.ValueWriter;

import ch.systemsx.cisd.hdf5.IHDF5Reader;

public class HDF5LongDataWriter extends HDF5DataWriter {

  private final long[] data;

  private final ValueWriter colWriter;

  // This constructor is used when the data is a 1D column.  The column is inferred from the datapath
  public HDF5LongDataWriter(IHDF5Reader reader, WriterSpec writerSpec, String datapath) {
    super(reader, datapath);
    data = reader.readLongArray(datapath);

    fieldName = HDF5Utils.getNameFromPath(datapath);
    colWriter = writerSpec.makeWriter(fieldName, TypeProtos.MinorType.BIGINT, TypeProtos.DataMode.OPTIONAL);
  }

  // This constructor is used when the data is part of a 2D array.  In this case the column name is provided in the constructor
  public HDF5LongDataWriter(IHDF5Reader reader, WriterSpec writerSpec, String datapath, String fieldName, int currentColumn) {
    super(reader, datapath, fieldName, currentColumn);
    // Get dimensions
    long[] dimensions = reader.object().getDataSetInformation(datapath).getDimensions();
    long[][] tempData;
    if (dimensions.length == 2) {
      tempData = transpose(reader.readLongMatrix(datapath));
    } else {
      tempData = transpose(reader.int64().readMDArray(datapath).toMatrix());
    }
    data = tempData[currentColumn];
    colWriter = writerSpec.makeWriter(fieldName, TypeProtos.MinorType.BIGINT, TypeProtos.DataMode.OPTIONAL);
  }

  public HDF5LongDataWriter(IHDF5Reader reader, WriterSpec writerSpec, String fieldName, List<Long> tempListData) {
    super(reader, null);
    this.fieldName = fieldName;
    data = new long[tempListData.size()];
    for (int i = 0; i < tempListData.size(); i++) {
      data[i] = tempListData.get(i);
    }
    colWriter = writerSpec.makeWriter(fieldName, TypeProtos.MinorType.BIGINT, TypeProtos.DataMode.OPTIONAL);
  }

  @Override
  public boolean write() {
    if (counter > data.length) {
      return false;
    } else {
      colWriter.setLong(data[counter++]);
      return true;
    }
  }

  @Override
  public boolean hasNext() {
    return counter < data.length;
  }

  /**
   * Transposes the input matrix by flipping a matrix over its diagonal by switching the row and column
   * indices of the matrix by producing another matrix.
   * @param matrix The input matrix to be transposed
   * @return The transposed matrix.
   */
  private long[][] transpose (long[][] matrix) {
    if (matrix == null || matrix.length == 0) {
      return matrix;
    }
    int width = matrix.length;
    int height = matrix[0].length;

    long[][] transposedMatrix = new long[height][width];

    for (int x = 0; x < width; x++) {
      for (int y = 0; y < height; y++) {
        transposedMatrix[y][x] = matrix[x][y];
      }
    }
    return transposedMatrix;
  }

  @Override
  public int getDataSize() {
    return data.length;
  }
}
