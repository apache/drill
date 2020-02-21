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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class HDF5MapDataWriter extends HDF5DataWriter {
  private static final Logger logger = LoggerFactory.getLogger(HDF5MapDataWriter.class);

  private static final String UNSAFE_SPACE_SEPARATOR = " ";

  private static final String SAFE_SPACE_SEPARATOR = "_";

  private final List<HDF5DataWriter> dataWriters;

  private List<String> fieldNames;

  public HDF5MapDataWriter(IHDF5Reader reader, WriterSpec writerSpec, String datapath) {
    super(reader, datapath);
    fieldNames = new ArrayList<>();

    compoundData = reader.compound().readArray(datapath, Object[].class);
    dataWriters = new ArrayList<>();
    fieldNames = getFieldNames();
    try {
      getDataWriters(writerSpec);
    } catch (Exception e) {
      throw UserException
        .dataReadError(e)
        .addContext("Error writing compound field", datapath)
        .addContext(writerSpec.errorContext)
        .build(logger);
    }
  }

  @Override
  public boolean write() {
    if (hasNext()) {
      // Loop through the columns and write the columns
      for (HDF5DataWriter dataWriter : dataWriters) {
        dataWriter.write();
      }
      counter++;
      return true;
    } else {
      return false;
    }
  }

  @Override
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
   * Populates the ArrayList of DataWriters. Since HDF5 Maps contain homogeneous
   * columns, it is fine to get the first row, and iterate through the columns
   * to get the data types and build the schema accordingly.
   */
  private void getDataWriters(WriterSpec writerSpec) {

    for (int col = 0; col < compoundData[0].length; col++) {
      Object currentColumn = compoundData[0][col];
      String dataType = currentColumn.getClass().getSimpleName();

      switch (dataType) {
        case "Byte":
        case "Short":
        case "Integer":
          dataWriters.add(new HDF5IntDataWriter(reader, writerSpec, fieldNames.get(col), getColumn(col)));
          break;
        case "Long":
          dataWriters.add(new HDF5LongDataWriter(reader, writerSpec, fieldNames.get(col), getColumn(col)));
          break;
        case "Double":
          dataWriters.add(new HDF5DoubleDataWriter(reader, writerSpec, fieldNames.get(col), getColumn(col)));
          break;
        case "Float":
          dataWriters.add(new HDF5FloatDataWriter(reader, writerSpec, fieldNames.get(col), getColumn(col)));
          break;
        case "String":
          dataWriters.add(new HDF5StringDataWriter(reader, writerSpec, fieldNames.get(col), getColumn(col)));
          break;
        default:
          // Log unknown data type
          logger.warn("Drill cannot process data type {} in compound fields.", dataType);
          break;
      }
    }
  }

  /**
   * Returns true if the data writer is a compound writer, false if not.
   * @return boolean true if the data writer is a compound writer, false if not.
   */
  @Override
  public boolean isCompound() {
    return true;
  }

  @Override
  public int getDataSize() {
    return dataWriters.size();
  }
}
