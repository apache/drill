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

package org.apache.drill.exec.store.hdf5;

import ch.systemsx.cisd.hdf5.HDF5CompoundMemberInformation;
import ch.systemsx.cisd.hdf5.HDF5DataSetInformation;
import ch.systemsx.cisd.hdf5.HDF5FactoryProvider;
import ch.systemsx.cisd.hdf5.HDF5LinkInformation;
import ch.systemsx.cisd.hdf5.IHDF5Factory;
import ch.systemsx.cisd.hdf5.IHDF5Reader;
import org.apache.commons.io.IOUtils;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.exec.physical.impl.scan.file.FileScanFramework.FileSchemaNegotiator;
import org.apache.drill.exec.physical.impl.scan.framework.ManagedReader;
import org.apache.drill.exec.physical.resultSet.ResultSetLoader;
import org.apache.drill.exec.physical.resultSet.RowSetLoader;
import org.apache.drill.exec.record.metadata.ColumnMetadata;
import org.apache.drill.exec.record.metadata.MapBuilder;
import org.apache.drill.exec.record.metadata.MetadataUtils;
import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.store.hdf5.writers.HDF5DataWriter;
import org.apache.drill.exec.store.hdf5.writers.HDF5DoubleDataWriter;
import org.apache.drill.exec.store.hdf5.writers.HDF5EnumDataWriter;
import org.apache.drill.exec.store.hdf5.writers.HDF5FloatDataWriter;
import org.apache.drill.exec.store.hdf5.writers.HDF5IntDataWriter;
import org.apache.drill.exec.store.hdf5.writers.HDF5LongDataWriter;
import org.apache.drill.exec.store.hdf5.writers.HDF5MapDataWriter;
import org.apache.drill.exec.store.hdf5.writers.HDF5StringDataWriter;
import org.apache.drill.exec.store.hdf5.writers.HDF5TimestampDataWriter;
import org.apache.drill.exec.vector.accessor.ArrayWriter;
import org.apache.drill.exec.vector.accessor.ScalarWriter;
import org.apache.drill.exec.vector.accessor.TupleWriter;

import org.apache.hadoop.mapred.FileSplit;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.BitSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class HDF5BatchReader implements ManagedReader<FileSchemaNegotiator> {
  private static final Logger logger = LoggerFactory.getLogger(HDF5BatchReader.class);

  private static final String PATH_COLUMN_NAME = "path";

  private static final String DATA_TYPE_COLUMN_NAME = "data_type";

  private static final String FILE_NAME_COLUMN_NAME = "file_name";

  private static final String INT_COLUMN_PREFIX = "int_col_";

  private static final String LONG_COLUMN_PREFIX = "long_col_";

  private static final String FLOAT_COLUMN_PREFIX = "float_col_";

  private static final String DOUBLE_COLUMN_PREFIX = "double_col_";

  private static final String INT_COLUMN_NAME = "int_data";

  private static final String FLOAT_COLUMN_NAME = "float_data";

  private static final String DOUBLE_COLUMN_NAME = "double_data";

  private static final String LONG_COLUMN_NAME = "long_data";

  private final HDF5ReaderConfig readerConfig;

  private final List<HDF5DataWriter> dataWriters;

  private FileSplit split;

  private IHDF5Reader hdf5Reader;

  private File inFile;

  private BufferedReader reader;

  private RowSetLoader rowWriter;

  private Iterator<HDF5DrillMetadata> metadataIterator;

  private ScalarWriter pathWriter;

  private ScalarWriter dataTypeWriter;

  private ScalarWriter fileNameWriter;

  private long[] dimensions;

  public static class HDF5ReaderConfig {
    final HDF5FormatPlugin plugin;

    final String defaultPath;

    final HDF5FormatConfig formatConfig;

    final File tempDirectory;

    public HDF5ReaderConfig(HDF5FormatPlugin plugin, HDF5FormatConfig formatConfig) {
      this.plugin = plugin;
      this.formatConfig = formatConfig;
      defaultPath = formatConfig.getDefaultPath();
      tempDirectory = plugin.getTmpDir();
    }
  }

  public HDF5BatchReader(HDF5ReaderConfig readerConfig) {
    this.readerConfig = readerConfig;
    dataWriters = new ArrayList<>();
  }

  @Override
  public boolean open(FileSchemaNegotiator negotiator) {
    split = negotiator.split();
    try {
      openFile(negotiator);
    } catch (IOException e) {
      throw UserException
        .dataReadError(e)
        .message("Failed to close input file: %s", split.getPath())
        .message(e.getMessage())
        .build(logger);
    }

    ResultSetLoader loader;
    if (readerConfig.defaultPath == null) {
      // Get file metadata
      List<HDF5DrillMetadata> metadata = getFileMetadata(hdf5Reader.object().getGroupMemberInformation("/", true), new ArrayList<>());
      metadataIterator = metadata.iterator();

      // Schema for Metadata query
      SchemaBuilder builder = new SchemaBuilder()
        .addNullable(PATH_COLUMN_NAME, TypeProtos.MinorType.VARCHAR)
        .addNullable(DATA_TYPE_COLUMN_NAME, TypeProtos.MinorType.VARCHAR)
        .addNullable(FILE_NAME_COLUMN_NAME, TypeProtos.MinorType.VARCHAR);

      negotiator.setTableSchema(builder.buildSchema(), false);

      loader = negotiator.build();
      dimensions = new long[0];
      rowWriter = loader.writer();

    } else {
      // This is the case when the default path is specified. Since the user is explicitly asking for a dataset
      // Drill can obtain the schema by getting the datatypes below and ultimately mapping that schema to columns
      HDF5DataSetInformation dsInfo = hdf5Reader.object().getDataSetInformation(readerConfig.defaultPath);
      dimensions = dsInfo.getDimensions();

      loader = negotiator.build();
      rowWriter = loader.writer();
      if (dimensions.length <= 1) {
        buildSchemaFor1DimensionalDataset(dsInfo);
      } else if (dimensions.length == 2) {
        buildSchemaFor2DimensionalDataset(dsInfo);
      } else {
        // Case for datasets of greater than 2D
        // These are automatically flattened
        buildSchemaFor2DimensionalDataset(dsInfo);
      }
    }
    if (readerConfig.defaultPath == null) {
      pathWriter = rowWriter.scalar(PATH_COLUMN_NAME);
      dataTypeWriter = rowWriter.scalar(DATA_TYPE_COLUMN_NAME);
      fileNameWriter = rowWriter.scalar(FILE_NAME_COLUMN_NAME);
    }
    return true;
  }

  /**
   * This function is called when the default path is set and the data set is a single dimension.
   * This function will create an array of one dataWriter of the
   * correct datatype
   * @param dsInfo The HDF5 dataset information
   */
  private void buildSchemaFor1DimensionalDataset(HDF5DataSetInformation dsInfo) {
    TypeProtos.MinorType currentDataType = HDF5Utils.getDataType(dsInfo);

    // Case for null or unknown data types:
    if (currentDataType == null) {
      logger.warn("Couldn't add {}", dsInfo.getTypeInformation().tryGetJavaType().toGenericString());
      return;
    }

    switch (currentDataType) {
      case GENERIC_OBJECT:
        dataWriters.add(new HDF5EnumDataWriter(hdf5Reader, rowWriter, readerConfig.defaultPath));
        break;
      case VARCHAR:
        dataWriters.add(new HDF5StringDataWriter(hdf5Reader, rowWriter, readerConfig.defaultPath));
        break;
      case TIMESTAMP:
        dataWriters.add(new HDF5TimestampDataWriter(hdf5Reader, rowWriter, readerConfig.defaultPath));
        break;
      case INT:
        dataWriters.add(new HDF5IntDataWriter(hdf5Reader, rowWriter, readerConfig.defaultPath));
        break;
      case BIGINT:
        dataWriters.add(new HDF5LongDataWriter(hdf5Reader, rowWriter, readerConfig.defaultPath));
        break;
      case FLOAT8:
        dataWriters.add(new HDF5DoubleDataWriter(hdf5Reader, rowWriter, readerConfig.defaultPath));
        break;
      case FLOAT4:
        dataWriters.add(new HDF5FloatDataWriter(hdf5Reader, rowWriter, readerConfig.defaultPath));
        break;
      case MAP:
        dataWriters.add(new HDF5MapDataWriter(hdf5Reader, rowWriter, readerConfig.defaultPath));
        break;
    }
  }

  /**
   * This function builds a Drill schema from a dataset with 2 or more dimensions.  HDF5 only supports INT, LONG, DOUBLE and FLOAT for >2 data types so this function is
   * not as inclusinve as the 1D function.  This function will build the schema by adding DataWriters to the dataWriters array.
   * @param dsInfo The dataset which Drill will use to build a schema
   */

  private void buildSchemaFor2DimensionalDataset(HDF5DataSetInformation dsInfo) {
    TypeProtos.MinorType currentDataType = HDF5Utils.getDataType(dsInfo);
    // Case for null or unknown data types:
    if (currentDataType == null) {
      logger.warn("Couldn't add {}", dsInfo.getTypeInformation().tryGetJavaType().toGenericString());
      return;
    }
    long cols = dimensions[1];

    String tempFieldName;
    for (int i = 0; i < cols; i++) {
      switch (currentDataType) {
        case INT:
          tempFieldName = INT_COLUMN_PREFIX + i;
          dataWriters.add(new HDF5IntDataWriter(hdf5Reader, rowWriter, readerConfig.defaultPath, tempFieldName, i));
          break;
        case BIGINT:
          tempFieldName = LONG_COLUMN_PREFIX + i;
          dataWriters.add(new HDF5LongDataWriter(hdf5Reader, rowWriter, readerConfig.defaultPath, tempFieldName, i));
          break;
        case FLOAT8:
          tempFieldName = DOUBLE_COLUMN_PREFIX + i;
          dataWriters.add(new HDF5DoubleDataWriter(hdf5Reader, rowWriter, readerConfig.defaultPath, tempFieldName, i));
          break;
        case FLOAT4:
          tempFieldName = FLOAT_COLUMN_PREFIX + i;
          dataWriters.add(new HDF5FloatDataWriter(hdf5Reader, rowWriter, readerConfig.defaultPath, tempFieldName, i));
          break;
      }
    }
  }
  /**
   * This function contains the logic to open an HDF5 file.
   * @param negotiator The negotiator represents Drill's interface with the file system
   */
  private void openFile(FileSchemaNegotiator negotiator) throws IOException {
    InputStream in = null;
    try {
      in = negotiator.fileSystem().openPossiblyCompressedStream(split.getPath());
      IHDF5Factory factory = HDF5FactoryProvider.get();
      inFile = convertInputStreamToFile(in);
      hdf5Reader = factory.openForReading(inFile);
    } catch (Exception e) {
      if (in != null) {
        in.close();
      }
      throw UserException
        .dataReadError(e)
        .message("Failed to open input file: %s", split.getPath())
        .build(logger);
    }
    reader = new BufferedReader(new InputStreamReader(in));
  }

  /**
   * This function converts the Drill InputStream into a File object for the HDF5 library. This function
   * exists due to a known limitation in the HDF5 library which cannot parse HDF5 directly from an input stream. A future
   * release of the library will support this.
   *
   * @param stream The input stream to be converted to a File
   * @return File The file which was converted from an InputStream
   */
  private File convertInputStreamToFile(InputStream stream) {
    File tmpDir = readerConfig.tempDirectory;
    String tempFileName = tmpDir.getPath() + "/~" + split.getPath().getName();
    File targetFile = new File(tempFileName);

    try {
      java.nio.file.Files.copy(stream, targetFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
    } catch (Exception e) {
      if (targetFile.exists()) {
        if (!targetFile.delete()) {
          logger.warn("{} not deleted.", targetFile.getName());
        }
      }
      throw UserException
        .dataWriteError(e)
        .message("Failed to create temp HDF5 file: %s", split.getPath())
        .addContext(e.getMessage())
        .build(logger);
    }

    IOUtils.closeQuietly(stream);
    return targetFile;
  }

  @Override
  public boolean next() {
    while (!rowWriter.isFull()) {
      if (readerConfig.defaultPath == null || readerConfig.defaultPath.isEmpty()) {
        if (!metadataIterator.hasNext()){
          return false;
        }
        projectMetadataRow(rowWriter);
      } else if (dimensions.length <= 1 && dataWriters.get(0).isCompound()) {
        if (!dataWriters.get(0).hasNext()) {
          return false;
        }
        dataWriters.get(0).write();
      } else if (dimensions.length <= 1) {
        // Case for Compound Data Type
        if (!dataWriters.get(0).hasNext()) {
          return false;
        }
        rowWriter.start();
        dataWriters.get(0).write();
        rowWriter.save();
      } else {
        int currentRowCount = 0;
        HDF5DataWriter currentDataWriter;
        rowWriter.start();

        for (int i = 0; i < dimensions[1]; i++) {
          currentDataWriter = dataWriters.get(i);
          currentDataWriter.write();
          currentRowCount = currentDataWriter.currentRowCount();
        }
        rowWriter.save();
        if (currentRowCount >= dimensions[0]) {
          return false;
        }
      }
    }
    return true;
  }

  /**
   * This function writes one row of HDF5 metadata.
   * @param rowWriter The input rowWriter object
   */
  private void projectMetadataRow(RowSetLoader rowWriter) {
    String realFileName = inFile.getName().replace("~", "");
    HDF5DrillMetadata metadataRow = metadataIterator.next();
    rowWriter.start();

    pathWriter.setString(metadataRow.getPath());
    dataTypeWriter.setString(metadataRow.getDataType());
    fileNameWriter.setString(realFileName);

    //Write attributes if present
    if (metadataRow.getAttributes().size() > 0) {
      writeAttributes(rowWriter, metadataRow);
    }

    if (metadataRow.getDataType().equalsIgnoreCase("DATASET")) {
      projectDataset(rowWriter, metadataRow.getPath());
    }
    rowWriter.save();
  }

  /**
   * This function gets the file metadata from a given HDF5 file.  It will extract the file name the path, and adds any information to the
   * metadata List.
   * @param members A list of paths from which the metadata will be extracted
   * @param metadata the HDF5 metadata object from which the metadata will be extracted
   * @return A list of metadata from the given file paths
   */
  private List<HDF5DrillMetadata> getFileMetadata(List<HDF5LinkInformation> members, List<HDF5DrillMetadata> metadata) {
    for (HDF5LinkInformation info : members) {
      HDF5DrillMetadata metadataRow = new HDF5DrillMetadata();

      metadataRow.setPath(info.getPath());
      metadataRow.setDataType(info.getType().toString());

      switch (info.getType()) {
        case DATASET:
          metadataRow.setAttributes(getAttributes(info.getPath()));
          metadata.add(metadataRow);
          break;
        case SOFT_LINK:
          // Soft links cannot have attributes
          metadata.add(metadataRow);
          break;
        case GROUP:
          metadataRow.setAttributes(getAttributes(info.getPath()));
          metadata.add(metadataRow);
          metadata = getFileMetadata(hdf5Reader.object().getGroupMemberInformation(info.getPath(), true), metadata);
          break;
        default:
          logger.warn("Unknown data type: {}", info.getType());
      }
    }
    return metadata;
  }

  /**
   * Gets the attributes of a HDF5 dataset and returns them into a HashMap
   *
   * @param path The path for which you wish to retrieve attributes
   * @return Map The attributes for the given path.  Empty Map if no attributes present
   */
  private Map<String, HDF5Attribute> getAttributes(String path) {
    Map<String, HDF5Attribute> attributes = new HashMap<>();

    long attrCount = hdf5Reader.object().getObjectInformation(path).getNumberOfAttributes();
    if (attrCount > 0) {
      List<String> attrNames = hdf5Reader.object().getAllAttributeNames(path);
      for (String name : attrNames) {
        try {
          HDF5Attribute attribute = HDF5Utils.getAttribute(path, name, hdf5Reader);
          if (attribute == null) {
            continue;
          }
          attributes.put(attribute.getKey(), attribute);
        } catch (Exception e) {
          logger.warn("Couldn't add attribute: {} - {}", path, name);
        }
      }
    }
    return attributes;
  }

  /**
   * This function writes one row of data in a metadata query. The number of dimensions here is n+1. So if the actual dataset is a 1D column, it will be written as a list.
   * This is function is only called in metadata queries as the schema is not known in advance.
   *
   * @param rowWriter The rowWriter to which the data will be written
   * @param datapath The datapath from which the data will be read
   */
  private void projectDataset(RowSetLoader rowWriter, String datapath) {
    String fieldName = HDF5Utils.getNameFromPath(datapath);
    IHDF5Reader reader = hdf5Reader;
    HDF5DataSetInformation dsInfo = reader.object().getDataSetInformation(datapath);
    long[] dimensions = dsInfo.getDimensions();
    //Case for single dimensional data
    if (dimensions.length <= 1) {
      TypeProtos.MinorType currentDataType = HDF5Utils.getDataType(dsInfo);

      assert currentDataType != null;
      switch (currentDataType) {
        case GENERIC_OBJECT:
          logger.warn("Couldn't read {}", datapath );
          break;
        case VARCHAR:
          String[] data = hdf5Reader.readStringArray(datapath);
          writeStringListColumn(rowWriter, fieldName, data);
          break;
        case TIMESTAMP:
          long[] longList = hdf5Reader.time().readTimeStampArray(datapath);
          writeTimestampListColumn(rowWriter, fieldName, longList);
          break;
        case INT:
          if (!dsInfo.getTypeInformation().isSigned()) {
            if (dsInfo.getTypeInformation().getElementSize() > 4) {
                longList = hdf5Reader.uint64().readArray(datapath);
                writeLongListColumn(rowWriter, fieldName, longList);
            }
          } else {
            int[] intList = hdf5Reader.readIntArray(datapath);
            writeIntListColumn(rowWriter, fieldName, intList);
          }
          break;
        case FLOAT4:
          float[] tempFloatList = hdf5Reader.readFloatArray(datapath);
          writeFloat4ListColumn(rowWriter, fieldName, tempFloatList);
          break;
        case FLOAT8:
          double[] tempDoubleList = hdf5Reader.readDoubleArray(datapath);
          writeFloat8ListColumn(rowWriter, fieldName, tempDoubleList);
          break;
        case BIGINT:
          if (!dsInfo.getTypeInformation().isSigned()) {
            logger.warn("Drill does not support unsigned 64bit integers.");
            break;
          }
          long[] tempBigIntList = hdf5Reader.readLongArray(datapath);
          writeLongListColumn(rowWriter, fieldName, tempBigIntList);
          break;
        case MAP:
          try {
            getAndMapCompoundData(datapath, new ArrayList<>(), hdf5Reader, rowWriter);
          } catch (Exception e) {
            throw UserException
              .dataReadError()
              .message("Error writing Compound Field: ")
              .addContext(e.getMessage())
              .build(logger);
          }
          break;

        default:
          // Case for data types that cannot be read
          logger.warn("{} not implemented yet.", dsInfo.getTypeInformation().tryGetJavaType());
      }
    } else if (dimensions.length == 2) {
      // Case for 2D data sets.  These are projected as lists of lists or maps of maps
      long cols = dimensions[1];
      long rows = dimensions[0];
      switch (HDF5Utils.getDataType(dsInfo)) {
        case INT:
          int[][] colData = hdf5Reader.readIntMatrix(datapath);
          mapIntMatrixField(colData, (int) cols, (int) rows, rowWriter);
          break;
        case FLOAT4:
          float[][] floatData = hdf5Reader.readFloatMatrix(datapath);
          mapFloatMatrixField(floatData, (int) cols, (int) rows, rowWriter);
          break;
        case FLOAT8:
          double[][] doubleData = hdf5Reader.readDoubleMatrix(datapath);
          mapDoubleMatrixField(doubleData, (int) cols, (int) rows, rowWriter);
          break;
        case BIGINT:
          long[][] longData = hdf5Reader.readLongMatrix(datapath);
          mapBigIntMatrixField(longData, (int) cols, (int) rows, rowWriter);
          break;
        default:
          logger.warn("{} not implemented.", HDF5Utils.getDataType(dsInfo));
      }
    } else {
      // Case for data sets with dimensions > 2
      long cols = dimensions[1];
      long rows = dimensions[0];
      switch (HDF5Utils.getDataType(dsInfo)) {
        case INT:
          int[][] colData = hdf5Reader.int32().readMDArray(datapath).toMatrix();
          mapIntMatrixField(colData, (int) cols, (int) rows, rowWriter);
          break;
        case FLOAT4:
          float[][] floatData = hdf5Reader.float32().readMDArray(datapath).toMatrix();
          mapFloatMatrixField(floatData, (int) cols, (int) rows, rowWriter);
          break;
        case FLOAT8:
          double[][] doubleData = hdf5Reader.float64().readMDArray(datapath).toMatrix();
          mapDoubleMatrixField(doubleData, (int) cols, (int) rows, rowWriter);
          break;
        case BIGINT:
          long[][] longData = hdf5Reader.int64().readMDArray(datapath).toMatrix();
          mapBigIntMatrixField(longData, (int) cols, (int) rows, rowWriter);
          break;
        default:
          logger.warn("{} not implemented.", HDF5Utils.getDataType(dsInfo));
      }
    }
  }

  /**
   * Helper function to write a 1D boolean column
   *
   * @param rowWriter The row to which the data will be written
   * @param name The column name
   * @param value The value to be written
   */
  private void writeBooleanColumn(TupleWriter rowWriter, String name, int value) {
    writeBooleanColumn(rowWriter, name, value != 0);
  }

  /**
   * Helper function to write a 1D boolean column
   *
   * @param rowWriter The row to which the data will be written
   * @param name The column name
   * @param value The value to be written
   */
  private void writeBooleanColumn(TupleWriter rowWriter, String name, boolean value) {
    ScalarWriter colWriter = getColWriter(rowWriter, name, TypeProtos.MinorType.BIT);
    colWriter.setBoolean(value);
  }

  /**
   * Helper function to write a 1D int column
   *
   * @param rowWriter The row to which the data will be written
   * @param name The column name
   * @param value The value to be written
   */
  private void writeIntColumn(TupleWriter rowWriter, String name, int value) {
    ScalarWriter colWriter = getColWriter(rowWriter, name, TypeProtos.MinorType.INT);
    colWriter.setInt(value);
  }

  /**
   * Helper function to write a 2D int list
   * @param rowWriter the row to which the data will be written
   * @param name the name of the outer list
   * @param list the list of data
   */
  private void writeIntListColumn(TupleWriter rowWriter, String name, int[] list) {
    int index = rowWriter.tupleSchema().index(name);
    if (index == -1) {
      ColumnMetadata colSchema = MetadataUtils.newScalar(name, TypeProtos.MinorType.INT, TypeProtos.DataMode.REPEATED);
      index = rowWriter.addColumn(colSchema);
    }

    ScalarWriter arrayWriter = rowWriter.column(index).array().scalar();
    for (int value : list) {
      arrayWriter.setInt(value);
    }
  }

  private void mapIntMatrixField(int[][] colData, int cols, int rows, RowSetLoader rowWriter) {
    // If the default path is not null, auto flatten the data
    // The end result are that a 2D array gets mapped to Drill columns
    if (readerConfig.defaultPath != null) {
      for (int i = 0; i < rows; i++) {
        rowWriter.start();
        for (int k = 0; k < cols; k++) {
          String tempColumnName = INT_COLUMN_PREFIX + k;
          writeIntColumn(rowWriter, tempColumnName, colData[i][k]);
        }
        rowWriter.save();
      }
    } else {
      intMatrixHelper(colData, cols, rows, rowWriter);
    }
  }

  private void intMatrixHelper(int[][] colData, int cols, int rows, RowSetLoader rowWriter) {
    // This is the case where a dataset is projected in a metadata query.  The result should be a list of lists

    TupleMetadata nestedSchema = new SchemaBuilder()
      .addRepeatedList(INT_COLUMN_NAME)
      .addArray(TypeProtos.MinorType.INT)
      .resumeSchema()
      .buildSchema();

    int index = rowWriter.tupleSchema().index(INT_COLUMN_NAME);
    if (index == -1) {
      index = rowWriter.addColumn(nestedSchema.column(INT_COLUMN_NAME));
    }

    // The outer array
    ArrayWriter listWriter = rowWriter.column(index).array();
    // The inner array
    ArrayWriter innerWriter = listWriter.array();
    // The strings within the inner array
    ScalarWriter intWriter = innerWriter.scalar();

    for (int i = 0; i < rows; i++) {
      for (int k = 0; k < cols; k++) {
        intWriter.setInt(colData[i][k]);
      }
      listWriter.save();
    }
  }

  private void writeLongColumn(TupleWriter rowWriter, String name, long value) {
    ScalarWriter colWriter = getColWriter(rowWriter, name, TypeProtos.MinorType.BIGINT);
    colWriter.setLong(value);
  }

  private void writeLongListColumn(TupleWriter rowWriter, String name, long[] list) {
    int index = rowWriter.tupleSchema().index(name);
    if (index == -1) {
      ColumnMetadata colSchema = MetadataUtils.newScalar(name, TypeProtos.MinorType.BIGINT, TypeProtos.DataMode.REPEATED);
      index = rowWriter.addColumn(colSchema);
    }

    ScalarWriter arrayWriter = rowWriter.column(index).array().scalar();
    for (long l : list) {
      arrayWriter.setLong(l);
    }
  }

  private void writeStringColumn(TupleWriter rowWriter, String name, String value) {
    ScalarWriter colWriter = getColWriter(rowWriter, name, TypeProtos.MinorType.VARCHAR);
    colWriter.setString(value);
  }

  private void writeStringListColumn(TupleWriter rowWriter, String name, String[] list) {
    int index = rowWriter.tupleSchema().index(name);
    if (index == -1) {
      ColumnMetadata colSchema = MetadataUtils.newScalar(name, TypeProtos.MinorType.VARCHAR, TypeProtos.DataMode.REPEATED);
      index = rowWriter.addColumn(colSchema);
    }

    ScalarWriter arrayWriter = rowWriter.column(index).array().scalar();
    for (String s : list) {
      arrayWriter.setString(s);
    }
  }

  private void writeFloat8Column(TupleWriter rowWriter, String name, double value) {
    ScalarWriter colWriter = getColWriter(rowWriter, name, TypeProtos.MinorType.FLOAT8);
    colWriter.setDouble(value);
  }

  private void writeFloat8ListColumn(TupleWriter rowWriter, String name, double[] list) {
    int index = rowWriter.tupleSchema().index(name);
    if (index == -1) {
      ColumnMetadata colSchema = MetadataUtils.newScalar(name, TypeProtos.MinorType.FLOAT8, TypeProtos.DataMode.REPEATED);
      index = rowWriter.addColumn(colSchema);
    }

    ScalarWriter arrayWriter = rowWriter.column(index).array().scalar();
    for (double v : list) {
      arrayWriter.setDouble(v);
    }
  }

  private void writeFloat4Column(TupleWriter rowWriter, String name, float value) {
    ScalarWriter colWriter = getColWriter(rowWriter, name, TypeProtos.MinorType.FLOAT4);
    colWriter.setDouble(value);
  }

  private void writeFloat4ListColumn(TupleWriter rowWriter, String name, float[] list) {
    int index = rowWriter.tupleSchema().index(name);
    if (index == -1) {
      ColumnMetadata colSchema = MetadataUtils.newScalar(name, TypeProtos.MinorType.FLOAT4, TypeProtos.DataMode.REPEATED);
      index = rowWriter.addColumn(colSchema);
    }

    ScalarWriter arrayWriter = rowWriter.column(index).array().scalar();
    for (float v : list) {
      arrayWriter.setDouble(v);
    }
  }

  private void mapFloatMatrixField(float[][] colData, int cols, int rows, RowSetLoader rowWriter) {
    // If the default path is not null, auto flatten the data
    // The end result are that a 2D array gets mapped to Drill columns
    if (readerConfig.defaultPath != null) {
      for (int i = 0; i < rows; i++) {
        rowWriter.start();
        for (int k = 0; k < cols; k++) {
          String tempColumnName = FLOAT_COLUMN_PREFIX + k;
          writeFloat4Column(rowWriter, tempColumnName, colData[i][k]);
        }
        rowWriter.save();
      }
    } else {
      floatMatrixHelper(colData, cols, rows, rowWriter);
    }
  }

  private void floatMatrixHelper(float[][] colData, int cols, int rows, RowSetLoader rowWriter) {
    // This is the case where a dataset is projected in a metadata query.  The result should be a list of lists

    TupleMetadata nestedSchema = new SchemaBuilder()
      .addRepeatedList(FLOAT_COLUMN_NAME)
      .addArray(TypeProtos.MinorType.FLOAT4)
      .resumeSchema()
      .buildSchema();

    int index = rowWriter.tupleSchema().index(FLOAT_COLUMN_NAME);
    if (index == -1) {
      index = rowWriter.addColumn(nestedSchema.column(FLOAT_COLUMN_NAME));
    }

    // The outer array
    ArrayWriter listWriter = rowWriter.column(index).array();
    // The inner array
    ArrayWriter innerWriter = listWriter.array();
    // The strings within the inner array
    ScalarWriter floatWriter = innerWriter.scalar();
    for (int i = 0; i < rows; i++) {
      for (int k = 0; k < cols; k++) {
        floatWriter.setDouble(colData[i][k]);
      }
      listWriter.save();
    }
  }

  private void mapDoubleMatrixField(double[][] colData, int cols, int rows, RowSetLoader rowWriter) {
    // If the default path is not null, auto flatten the data
    // The end result are that a 2D array gets mapped to Drill columns
    if (readerConfig.defaultPath != null) {
      for (int i = 0; i < rows; i++) {
        rowWriter.start();
        for (int k = 0; k < cols; k++) {
          String tempColumnName = DOUBLE_COLUMN_PREFIX + k;
          writeFloat8Column(rowWriter, tempColumnName, colData[i][k]);
        }
        rowWriter.save();
      }
    } else {
      doubleMatrixHelper(colData, cols, rows, rowWriter);
    }
  }

  private void doubleMatrixHelper(double[][] colData, int cols, int rows, RowSetLoader rowWriter) {
    // This is the case where a dataset is projected in a metadata query.  The result should be a list of lists

    TupleMetadata nestedSchema = new SchemaBuilder()
      .addRepeatedList(DOUBLE_COLUMN_NAME)
      .addArray(TypeProtos.MinorType.FLOAT8)
      .resumeSchema()
      .buildSchema();

    int index = rowWriter.tupleSchema().index(DOUBLE_COLUMN_NAME);
    if (index == -1) {
      index = rowWriter.addColumn(nestedSchema.column(DOUBLE_COLUMN_NAME));
    }

    // The outer array
    ArrayWriter listWriter = rowWriter.column(index).array();
    // The inner array
    ArrayWriter innerWriter = listWriter.array();
    // The strings within the inner array
    ScalarWriter floatWriter = innerWriter.scalar();

    for (int i = 0; i < rows; i++) {
      for (int k = 0; k < cols; k++) {
        floatWriter.setDouble(colData[i][k]);
      }
      listWriter.save();
    }
  }

  private void mapBigIntMatrixField(long[][] colData, int cols, int rows, RowSetLoader rowWriter) {
    // If the default path is not null, auto flatten the data
    // The end result are that a 2D array gets mapped to Drill columns
    if (readerConfig.defaultPath != null) {
      for (int i = 0; i < rows; i++) {
        rowWriter.start();
        for (int k = 0; k < cols; k++) {
          String tempColumnName = LONG_COLUMN_PREFIX + k;
          writeLongColumn(rowWriter, tempColumnName, colData[i][k]);
        }
        rowWriter.save();
      }
    } else {
      bigIntMatrixHelper(colData, cols, rows, rowWriter);
    }
  }

  private void bigIntMatrixHelper(long[][] colData, int cols, int rows, RowSetLoader rowWriter) {
    // This is the case where a dataset is projected in a metadata query.  The result should be a list of lists

    TupleMetadata nestedSchema = new SchemaBuilder()
      .addRepeatedList(LONG_COLUMN_NAME)
      .addArray(TypeProtos.MinorType.BIGINT)
      .resumeSchema()
      .buildSchema();

    int index = rowWriter.tupleSchema().index(LONG_COLUMN_NAME);
    if (index == -1) {
      index = rowWriter.addColumn(nestedSchema.column(LONG_COLUMN_NAME));
    }

    // The outer array
    ArrayWriter listWriter = rowWriter.column(index).array();
    // The inner array
    ArrayWriter innerWriter = listWriter.array();
    // The strings within the inner array
    ScalarWriter bigintWriter = innerWriter.scalar();

    for (int i = 0; i < rows; i++) {
      for (int k = 0; k < cols; k++) {
        bigintWriter.setLong(colData[i][k]);
      }
      listWriter.save();
    }
  }

  private void writeTimestampColumn(TupleWriter rowWriter, String name, long timestamp) {
    Instant ts = new Instant(timestamp);
    ScalarWriter colWriter = getColWriter(rowWriter, name, TypeProtos.MinorType.TIMESTAMP);
    colWriter.setTimestamp(ts);
  }

  private void writeTimestampListColumn(TupleWriter rowWriter, String name, long[] list) {
    int index = rowWriter.tupleSchema().index(name);
    if (index == -1) {
      ColumnMetadata colSchema = MetadataUtils.newScalar(name, TypeProtos.MinorType.TIMESTAMP, TypeProtos.DataMode.REPEATED);
      index = rowWriter.addColumn(colSchema);
    }

    ScalarWriter arrayWriter = rowWriter.column(index).array().scalar();
    for (long l : list) {
      arrayWriter.setTimestamp(new Instant(l));
    }
  }

  private ScalarWriter getColWriter(TupleWriter tupleWriter, String fieldName, TypeProtos.MinorType type) {
    int index = tupleWriter.tupleSchema().index(fieldName);
    if (index == -1) {
      ColumnMetadata colSchema = MetadataUtils.newScalar(fieldName, type, TypeProtos.DataMode.OPTIONAL);
      index = tupleWriter.addColumn(colSchema);
    }
    return tupleWriter.scalar(index);
  }

  /**
   * This helper function gets the attributes for an HDF5 datapath.  These attributes are projected as a map in select * queries when the defaultPath is null.
   * @param rowWriter the row to which the data will be written
   * @param record the record for the attributes
   */
  private void writeAttributes(TupleWriter rowWriter, HDF5DrillMetadata record) {
    Map<String, HDF5Attribute> attribs = getAttributes(record.getPath());
    Iterator<Map.Entry<String, HDF5Attribute>> entries = attribs.entrySet().iterator();

    int index = rowWriter.tupleSchema().index("attributes");
    if (index == -1) {
      index = rowWriter
        .addColumn(SchemaBuilder.columnSchema("attributes", TypeProtos.MinorType.MAP, TypeProtos.DataMode.REQUIRED));
    }
    TupleWriter mapWriter = rowWriter.tuple(index);

    while (entries.hasNext()) {
      Map.Entry<String, HDF5Attribute> entry = entries.next();
      String key = entry.getKey();

      HDF5Attribute attrib = entry.getValue();
      switch (attrib.getDataType()) {
        case BIT:
          boolean value = (Boolean) attrib.getValue();
          writeBooleanColumn(mapWriter, key, value);
          break;
        case BIGINT:
          writeLongColumn(mapWriter, key, (Long) attrib.getValue());
          break;
        case INT:
          writeIntColumn(mapWriter, key, (Integer) attrib.getValue());
          break;
        case FLOAT8:
          writeFloat8Column(mapWriter, key, (Double) attrib.getValue());
          break;
        case FLOAT4:
          writeFloat4Column(mapWriter, key, (Float) attrib.getValue());
          break;
        case VARCHAR:
          writeStringColumn(mapWriter, key, (String) attrib.getValue());
          break;
        case TIMESTAMP:
          writeTimestampColumn(mapWriter, key, (Long) attrib.getValue());
        case GENERIC_OBJECT:
          //This is the case for HDF5 enums
          String enumText = attrib.getValue().toString();
          writeStringColumn(mapWriter, key, enumText);
          break;
      }
    }
  }

  /**
   * This function processes the MAP data type which can be found in HDF5 files.
   * It automatically flattens anything greater than 2 dimensions.
   *
   * @param path the HDF5 path tp the compound data
   * @param fieldNames The field names to be mapped
   * @param reader the HDF5 reader for the data file
   * @param rowWriter the rowWriter to write the data
   */

  private void getAndMapCompoundData(String path, List<String> fieldNames, IHDF5Reader reader, RowSetLoader rowWriter) {

    final String COMPOUND_DATA_FIELD_NAME = "compound_data";
    String resolvedPath = HDF5Utils.resolvePath(path, reader);
    Class<?> dataClass = HDF5Utils.getDatasetClass(resolvedPath, reader);
    if (dataClass == Map.class) {
      Object[][] values = reader.compound().readArray(resolvedPath, Object[].class);
      String currentFieldName;

      if (fieldNames != null) {
        HDF5CompoundMemberInformation[] infos = reader.compound().getDataSetInfo(resolvedPath);
        for (HDF5CompoundMemberInformation info : infos) {
          fieldNames.add(info.getName());
        }
      }

      // Case for auto-flatten
      if (readerConfig.defaultPath != null) {
        for (int row = 0; row < values.length; row++) {
          rowWriter.start();
          for (int col = 0; col < values[row].length; col++) {

            assert fieldNames != null;
            currentFieldName = fieldNames.get(col);

            if (values[row][col] instanceof Integer) {
              writeIntColumn(rowWriter, currentFieldName, (Integer) values[row][col]);
            } else if (values[row][col] instanceof Short) {
              writeIntColumn(rowWriter, currentFieldName, ((Short) values[row][col]).intValue());
            } else if (values[row][col] instanceof Byte) {
              writeIntColumn(rowWriter, currentFieldName, ((Byte) values[row][col]).intValue());
            } else if (values[row][col] instanceof Long) {
              writeLongColumn(rowWriter, currentFieldName, (Long) values[row][col]);
            } else if (values[row][col] instanceof Float) {
              writeFloat4Column(rowWriter, currentFieldName, (Float) values[row][col]);
            } else if (values[row][col] instanceof Double) {
              writeFloat8Column(rowWriter, currentFieldName, (Double) values[row][col]);
            } else if (values[row][col] instanceof BitSet || values[row][col] instanceof Boolean) {
              assert values[row][col] instanceof Integer;
              writeBooleanColumn(rowWriter, currentFieldName, (Integer) values[row][col]);
            } else if (values[row][col] instanceof String) {
              String stringValue = (String) values[row][col];
              writeStringColumn(rowWriter, currentFieldName, stringValue);
            }
          }
          rowWriter.save();
        }
      } else {
        int index = 0;
        if (fieldNames != null) {
          HDF5CompoundMemberInformation[] infos = reader.compound().getDataSetInfo(resolvedPath);

          SchemaBuilder innerSchema = new SchemaBuilder();
          MapBuilder mapBuilder = innerSchema.addMap(COMPOUND_DATA_FIELD_NAME);

          for (HDF5CompoundMemberInformation info : infos) {
            fieldNames.add(info.getName());

            switch (info.getType().tryGetJavaType().getSimpleName()) {
              case "int":
                mapBuilder.add(info.getName(), TypeProtos.MinorType.INT, TypeProtos.DataMode.REPEATED);
                break;
              case "double":
                mapBuilder.add(info.getName(), TypeProtos.MinorType.FLOAT8, TypeProtos.DataMode.REPEATED);
                break;
              case "float":
                mapBuilder.add(info.getName(), TypeProtos.MinorType.FLOAT4, TypeProtos.DataMode.REPEATED);
                break;
              case "long":
                mapBuilder.add(info.getName(), TypeProtos.MinorType.BIGINT, TypeProtos.DataMode.REPEATED);
                break;
              case "String":
                mapBuilder.add(info.getName(), TypeProtos.MinorType.VARCHAR, TypeProtos.DataMode.REPEATED);
                break;
            }
          }
          TupleMetadata finalInnerSchema = mapBuilder.resumeSchema().buildSchema();

          index = rowWriter.tupleSchema().index(COMPOUND_DATA_FIELD_NAME);
          if (index == -1) {
            index = rowWriter.addColumn(finalInnerSchema.column(COMPOUND_DATA_FIELD_NAME));
          }
        }
        TupleWriter listWriter = rowWriter.column(index).tuple();

        //Note: The [][] returns an array of [rows][cols]
        for (int row = 0; row < values.length; row++) {
          // Iterate over rows
          for (int col = 0; col < values[row].length; col++) {
            assert fieldNames != null;
            currentFieldName = fieldNames.get(col);
            ArrayWriter innerWriter = listWriter.array(currentFieldName);
            if (values[row][col] instanceof Integer) {
              innerWriter.scalar().setInt((Integer) values[row][col]);
            } else if (values[row][col] instanceof Short) {
              innerWriter.scalar().setInt((Short) values[row][col]);
            } else if (values[row][col] instanceof Byte) {
              innerWriter.scalar().setInt((Byte) values[row][col]);
            } else if (values[row][col] instanceof Long) {
              innerWriter.scalar().setLong((Long) values[row][col]);
            } else if (values[row][col] instanceof Float) {
              innerWriter.scalar().setDouble((Float) values[row][col]);
            } else if (values[row][col] instanceof Double) {
              innerWriter.scalar().setDouble((Double) values[row][col]);
            } else if (values[row][col] instanceof BitSet || values[row][col] instanceof Boolean) {
              innerWriter.scalar().setBoolean((Boolean) values[row][col]);
            } else if (values[row][col] instanceof String) {
              innerWriter.scalar().setString((String) values[row][col]);
            }
            if (col == values[row].length) {
              innerWriter.save();
            }
          }
        }
      }
    }
  }

  @Override
  public void close() {
    if (hdf5Reader != null) {
      hdf5Reader.close();
      hdf5Reader = null;
    }
    if (reader != null) {
      try {
        reader.close();
      } catch (IOException e) {
        logger.debug("Failed to close HDF5 Reader.");
      }
      reader = null;
    }
    if (inFile != null) {
      if (!inFile.delete()) {
        logger.warn("{} file not deleted.", inFile.getName());
      }
      inFile = null;
    }
  }
}
