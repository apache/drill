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
import org.apache.drill.exec.store.hdf5.HDF5Utils;
import org.apache.drill.exec.vector.accessor.ScalarWriter;
import java.util.List;

public class HDF5IntDataWriter extends HDF5DataWriter {

  private final int[] data;

  private final ScalarWriter rowWriter;

  // This constructor is used when the data is a 1D column.  The column is inferred from the datapath
  public HDF5IntDataWriter(IHDF5Reader reader, RowSetLoader columnWriter, String datapath) {
    super(reader, columnWriter, datapath);
    data = reader.readIntArray(datapath);

    fieldName = HDF5Utils.getNameFromPath(datapath);

    rowWriter = makeWriter(columnWriter, fieldName, TypeProtos.MinorType.INT, TypeProtos.DataMode.OPTIONAL);
  }

  // This constructor is used when the data is part of a 2D array.  In this case the column name is provided in the constructor
  public HDF5IntDataWriter(IHDF5Reader reader, RowSetLoader columnWriter, String datapath, String fieldName, int currentColumn) {
    super(reader, columnWriter, datapath, fieldName, currentColumn);
    // Get dimensions
    long[] dimensions = reader.object().getDataSetInformation(datapath).getDimensions();
    int[][] tempData;
    if (dimensions.length == 2) {
      tempData = transpose(reader.readIntMatrix(datapath));
    } else {
      tempData = transpose(reader.int32().readMDArray(datapath).toMatrix());
    }
    data = tempData[currentColumn];

    rowWriter = makeWriter(columnWriter, fieldName, TypeProtos.MinorType.INT, TypeProtos.DataMode.OPTIONAL);
  }

  // This constructor is used for compound data types.
  public HDF5IntDataWriter(IHDF5Reader reader, RowSetLoader columnWriter, String fieldName, List<Integer> tempListData) {
    super(reader, columnWriter, null);
    this.fieldName = fieldName;
    data = new int[tempListData.size()];
    for (int i = 0; i < tempListData.size(); i++) {
      data[i] = tempListData.get(i);
    }

    rowWriter = makeWriter(columnWriter, fieldName, TypeProtos.MinorType.INT, TypeProtos.DataMode.OPTIONAL);
  }


  public boolean write() {
    if (counter > data.length) {
      return false;
    } else {
      rowWriter.setInt(data[counter++]);
      return true;
    }
  }

  public boolean hasNext() {
    return counter < data.length;
  }

  /**
   * Transposes the input matrix by flipping a matrix over its diagonal by switching the row and column
   * indices of the matrix by producing another matrix.
   * @param matrix The input matrix to be transposed
   * @return The transposed matrix.
   */
  private int[][] transpose (int[][] matrix) {
    if (matrix == null || matrix.length == 0) {
      return matrix;
    }
    int width = matrix.length;
    int height = matrix[0].length;

    int[][] transposedMatrix = new int[height][width];

    for (int x = 0; x < width; x++) {
      for (int y = 0; y < height; y++) {
        transposedMatrix[y][x] = matrix[x][y];
      }
    }
    return transposedMatrix;
  }

  public int getDataSize() {
    return data.length;
  }
}
