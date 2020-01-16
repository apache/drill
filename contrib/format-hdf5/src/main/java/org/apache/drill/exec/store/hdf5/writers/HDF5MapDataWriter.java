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

import ch.systemsx.cisd.hdf5.HDF5CompoundMemberInformation;
import ch.systemsx.cisd.hdf5.IHDF5Reader;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.physical.resultSet.RowSetLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class HDF5MapDataWriter extends HDF5DataWriter {
  private static final Logger logger = LoggerFactory.getLogger(HDF5MapDataWriter.class);

  private final String UNSAFE_SPACE_SEPARATOR = " ";

  private final String SAFE_SPACE_SEPARATOR = "_";

  private final List<HDF5DataWriter> dataWriters;

  private List<String> fieldNames;


  public HDF5MapDataWriter(IHDF5Reader reader, RowSetLoader columnWriter, String datapath) {
    super(reader, columnWriter, datapath);
    fieldNames = new ArrayList<>();

    compoundData = reader.compound().readArray(datapath, Object[].class);
    dataWriters = new ArrayList<>();
    fieldNames = getFieldNames();
    try {
      getDataWriters();
    } catch (Exception e) {
      throw UserException
        .dataReadError()
        .message("Error writing Compound Field: %s", e.getMessage())
        .build(logger);
    }
  }

  public boolean write() {
    if (!hasNext()) {
      return false;
    } else {
      // Loop through the columns and write the columns
      columnWriter.start();
      for (HDF5DataWriter dataWriter : dataWriters) {
        dataWriter.write();
      }
      columnWriter.save();
      counter++;
      return true;
    }
  }

  public boolean hasNext() {
    return counter < dataWriters.get(0).getDataSize();
  }

  private List<String> getFieldNames() {
    List<String> names = new ArrayList<>();

    HDF5CompoundMemberInformation[] infos = reader.compound().getDataSetInfo(datapath);
    for (HDF5CompoundMemberInformation info : infos) {
      names.add(info.getName().replace(UNSAFE_SPACE_SEPARATOR, SAFE_SPACE_SEPARATOR));
    }
    return names;
  }

  /**
   * This function will populate the ArrayList of DataWriters. Since HDF5 Maps contain homogeneous columns,
   * it is fine to get the first row, and iterate through the columns to get the data types and build the schema accordingly.
   */
  private void getDataWriters() {
    List listData;

    for (int col = 0; col < compoundData[0].length; col++) {
      Object currentColumn = compoundData[0][col];
      String dataType = currentColumn.getClass().getSimpleName();
      listData = getColumn(col);

      switch (dataType) {
        case "Byte":
        case "Short":
        case "Integer":
          dataWriters.add(new HDF5IntDataWriter(reader, columnWriter, fieldNames.get(col), listData));
          break;
        case "Long":
          dataWriters.add(new HDF5LongDataWriter(reader, columnWriter, fieldNames.get(col), listData));
          break;
        case "Double":
          dataWriters.add(new HDF5DoubleDataWriter(reader, columnWriter, fieldNames.get(col), listData));
          break;
        case "Float":
          dataWriters.add(new HDF5FloatDataWriter(reader, columnWriter, fieldNames.get(col), listData));
          break;
        case "String":
          dataWriters.add(new HDF5StringDataWriter(reader, columnWriter, fieldNames.get(col), listData));
          break;
        default:
          // Log unknown data type
          logger.warn("Drill cannot process data type {} in compound fields.", dataType);
          break;
      }
    }
  }

  /**
   * This function returns true if the data writer is a compound writer, false if not.
   * @return boolean true if the data writer is a compound writer, false if not.
   */
  public boolean isCompound() {
    return true;
  }

  @Override
  public int getDataSize() {
    return dataWriters.size();
  }
}
