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
package org.apache.drill.exec.store.easy.json;

import com.fasterxml.jackson.core.JsonFactory;
import java.io.IOException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.metastore.statistics.Statistic;
import org.apache.drill.exec.planner.common.DrillStatsTable;
import org.apache.drill.exec.planner.common.DrillStatsTable.STATS_VERSION;
import org.apache.drill.exec.record.VectorAccessible;
import org.apache.drill.exec.store.EventBasedRecordWriter.FieldConverter;
import org.apache.drill.exec.store.JSONBaseStatisticsRecordWriter;
import org.apache.drill.exec.store.dfs.FormatPlugin;
import org.apache.drill.exec.util.ImpersonationUtil;
import org.apache.drill.exec.vector.complex.reader.FieldReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class JsonStatisticsRecordWriter extends JSONBaseStatisticsRecordWriter {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(JsonStatisticsRecordWriter.class);
  private static final String LINE_FEED = String.format("%n");
  private String location;
  private String prefix;
  private String fieldDelimiter;
  private String extension;
  private boolean useExtendedOutput;
  private FileSystem fs = null;
  private STATS_VERSION statisticsVersion;
  private final JsonFactory factory = new JsonFactory();
  private String lastDirectory = null;
  private Configuration fsConf = null;
  private FormatPlugin formatPlugin = null;
  private String nextField = null;
  private DrillStatsTable.TableStatistics statistics;
  private List<DrillStatsTable.ColumnStatistics> columnStatisticsList = new ArrayList<DrillStatsTable.ColumnStatistics>();
  private DrillStatsTable.ColumnStatistics columnStatistics;
  private LocalDate dirComputedTime = null;
  private Path fileName = null;
  private String queryId = null;
  private long recordsWritten = -1;
  private boolean errStatus = false;

  public JsonStatisticsRecordWriter(Configuration fsConf, FormatPlugin formatPlugin){
    this.fsConf = fsConf;
    this.formatPlugin = formatPlugin;
  }

  @Override
  public void init(Map<String, String> writerOptions) throws IOException {
    this.location = writerOptions.get("location");
    this.prefix = writerOptions.get("prefix");
    this.fieldDelimiter = writerOptions.get("separator");
    this.extension = writerOptions.get("extension");
    this.useExtendedOutput = Boolean.parseBoolean(writerOptions.get("extended"));
    this.skipNullFields = Boolean.parseBoolean(writerOptions.get("skipnulls"));
    this.statisticsVersion = DrillStatsTable.CURRENT_VERSION;
    this.queryId = writerOptions.get("queryid");
     //Write as DRILL process user
    this.fs = ImpersonationUtil.createFileSystem(ImpersonationUtil.getProcessUserName(), fsConf);

    fileName = new Path(location, prefix + "." + extension + ".tmp." + queryId);
    // Delete .tmp file if exists. Unexpected error in cleanup during last ANALYZE
    try {
      if (fs.exists(fileName)) {
        fs.delete(fileName, false);
      }
    } catch (IOException ex) {
      logger.error("Unable to delete tmp file (corrupt): " + fileName, ex);
      throw ex;
    }
    try {
      // Delete the tmp file and .stats.drill on exit. After writing out the permanent file
      // we cancel the deleteOnExit. This ensures that if prior to writing out the stats
      // file the process is killed, we perform the cleanup.
      fs.deleteOnExit(fileName);
      fs.deleteOnExit(new Path(location));
      logger.debug("Created file: {}", fileName);
    } catch (IOException ex) {
      logger.error("Unable to create file: " + fileName, ex);
      throw ex;
    }
  }

  @Override
  public void updateSchema(VectorAccessible batch) throws IOException {
    // no op
  }

  @Override
  public boolean isBlockingWriter() {
    return true;
  }

  @Override
  public void checkForNewPartition(int index) {
    // no op
  }

  @Override
  public FieldConverter getNewBigIntConverter(int fieldId, String fieldName, FieldReader reader) {
    return new BigIntJsonConverter(fieldId, fieldName, reader);
  }

  public class BigIntJsonConverter extends FieldConverter {

    public BigIntJsonConverter(int fieldId, String fieldName, FieldReader reader) {
      super(fieldId, fieldName, reader);
    }

    @Override
    public void startField() throws IOException {
      if (fieldName.equals(Statistic.SCHEMA)) {
        nextField = fieldName;
      } else if (fieldName.equals(Statistic.ROWCOUNT)
            || fieldName.equals(Statistic.NNROWCOUNT)
            || fieldName.equals(Statistic.NDV)
            || fieldName.equals(Statistic.AVG_WIDTH)
            || fieldName.equals(Statistic.SUM_DUPS)) {
        nextField = fieldName;
      }
    }

    @Override
    public void writeField() throws IOException {
      if (nextField == null) {
        errStatus = true;
        throw new IOException("Statistics writer encountered unexpected field");
      }
      if (nextField.equals(Statistic.SCHEMA)) {
        ((DrillStatsTable.ColumnStatistics_v1) columnStatistics).setSchema(reader.readLong());
      } else if (nextField.equals(Statistic.ROWCOUNT)) {
        ((DrillStatsTable.ColumnStatistics_v1) columnStatistics).setCount(reader.readLong());
      } else if (nextField.equals(Statistic.NNROWCOUNT)) {
        ((DrillStatsTable.ColumnStatistics_v1) columnStatistics).setNonNullCount(reader.readLong());
      } else if (nextField.equals(Statistic.NDV)) {
        ((DrillStatsTable.ColumnStatistics_v1) columnStatistics).setNdv(reader.readLong());
      } else if (nextField.equals(Statistic.AVG_WIDTH)) {
        ((DrillStatsTable.ColumnStatistics_v1) columnStatistics).setAvgWidth(reader.readLong());
      } else if (nextField.equals(Statistic.SUM_DUPS)) {
        // Ignore Count_Approx_Dups statistic
      }
    }

    @Override
    public void endField() throws IOException {
      nextField = null;
    }
  }

  @Override
  public FieldConverter getNewIntConverter(int fieldId, String fieldName, FieldReader reader) {
    return new IntJsonConverter(fieldId, fieldName, reader);
  }

  public class IntJsonConverter extends FieldConverter {

    public IntJsonConverter(int fieldId, String fieldName, FieldReader reader) {
      super(fieldId, fieldName, reader);
    }

    @Override
    public void startField() throws IOException {
      if (fieldName.equals(Statistic.COLTYPE)) {
        nextField = fieldName;
      }
    }

    @Override
    public void writeField() throws IOException {
      if (nextField == null) {
        errStatus = true;
        throw new IOException("Statistics writer encountered unexpected field");
      }
      if (nextField.equals(Statistic.COLTYPE)) {
        // Do not write out the type
      }
    }

    @Override
    public void endField() throws IOException {
      nextField = null;
    }
  }

  @Override
  public FieldConverter getNewDateConverter(int fieldId, String fieldName, FieldReader reader) {
    return new DateJsonConverter(fieldId, fieldName, reader);
  }

  public class DateJsonConverter extends FieldConverter {

    public DateJsonConverter(int fieldId, String fieldName, FieldReader reader) {
      super(fieldId, fieldName, reader);
    }

    @Override
    public void startField() throws IOException {
      if (fieldName.equals(Statistic.COMPUTED)) {
        nextField = fieldName;
      }
    }

    @Override
    public void writeField() throws IOException {
      if (nextField == null) {
        errStatus = true;
        throw new IOException("Statistics writer encountered unexpected field");
      }
      if (nextField.equals((Statistic.COMPUTED))) {
        LocalDate computedTime = reader.readLocalDate();
        if (dirComputedTime == null
               || computedTime.compareTo(dirComputedTime) > 0) {
          dirComputedTime = computedTime;
        }
      }
    }

    @Override
    public void endField() throws IOException {
      nextField = null;
    }
  }

  @Override
  public FieldConverter getNewVarCharConverter(int fieldId, String fieldName, FieldReader reader) {
    return new VarCharJsonConverter(fieldId, fieldName, reader);
  }

  public class VarCharJsonConverter extends FieldConverter {

    public VarCharJsonConverter(int fieldId, String fieldName, FieldReader reader) {
      super(fieldId, fieldName, reader);
    }

    @Override
    public void startField() throws IOException {
      if (fieldName.equals(Statistic.COLNAME)) {
        nextField = fieldName;
      } else if (fieldName.equals(Statistic.COLTYPE)) {
        nextField = fieldName;
      }
    }

    @Override
    public void writeField() throws IOException {
      if (nextField == null) {
        errStatus = true;
        throw new IOException("Statistics writer encountered unexpected field");
      }
      if (nextField.equals(Statistic.COLNAME)) {
        ((DrillStatsTable.ColumnStatistics_v1) columnStatistics).setName(SchemaPath.parseFromString(reader.readText().toString()));
      } else if (nextField.equals(Statistic.COLTYPE)) {
        MajorType fieldType = DrillStatsTable.getMapper().readValue(reader.readText().toString(), MajorType.class);
        ((DrillStatsTable.ColumnStatistics_v1) columnStatistics).setType(fieldType);
      }
    }

    @Override
    public void endField() {
      nextField = null;
    }
  }

  @Override
  public FieldConverter getNewNullableBigIntConverter(int fieldId, String fieldName, FieldReader reader) {
    return new NullableBigIntJsonConverter(fieldId, fieldName, reader);
  }

  public class NullableBigIntJsonConverter extends FieldConverter {

    public NullableBigIntJsonConverter(int fieldId, String fieldName, FieldReader reader) {
      super(fieldId, fieldName, reader);
    }

    @Override
    public void startField() throws IOException {
      if (!skipNullFields || this.reader.isSet()) {
        if (fieldName.equals(Statistic.ROWCOUNT)
            || fieldName.equals(Statistic.NNROWCOUNT)
            || fieldName.equals(Statistic.NDV)
            || fieldName.equals(Statistic.SUM_DUPS)) {
          nextField = fieldName;
        }
      }
    }

    @Override
    public void writeField() throws IOException {
      if (!skipNullFields || this.reader.isSet()) {
        if (nextField == null) {
          errStatus = true;
          throw new IOException("Statistics writer encountered unexpected field");
        }
        if (nextField.equals(Statistic.ROWCOUNT)) {
          ((DrillStatsTable.ColumnStatistics_v1) columnStatistics).setCount(reader.readLong());
        } else if (nextField.equals(Statistic.NNROWCOUNT)) {
          ((DrillStatsTable.ColumnStatistics_v1) columnStatistics).setNonNullCount(reader.readLong());
        } else if (nextField.equals(Statistic.NDV)) {
          ((DrillStatsTable.ColumnStatistics_v1) columnStatistics).setNdv(reader.readLong());
        } else if (nextField.equals(Statistic.SUM_DUPS)) {
          // Ignore Count_Approx_Dups statistic
        }
      }
    }

    @Override
    public void endField() throws IOException {
      nextField = null;
    }
  }

  @Override
  public FieldConverter getNewNullableVarBinaryConverter(int fieldId, String fieldName, FieldReader reader) {
    return new NullableVarBinaryJsonConverter(fieldId, fieldName, reader);
  }

  public class NullableVarBinaryJsonConverter extends FieldConverter {

    public NullableVarBinaryJsonConverter(int fieldId, String fieldName, FieldReader reader) {
      super(fieldId, fieldName, reader);
    }

    @Override
    public void startField() throws IOException {
      if (!skipNullFields || this.reader.isSet()) {
        if (fieldName.equals(Statistic.HLL)
            || fieldName.equals(Statistic.HLL_MERGE)
            || fieldName.equals(Statistic.TDIGEST_MERGE)) {
          nextField = fieldName;
        }
      }
    }

    @Override
    public void writeField() throws IOException {
      if (!skipNullFields || this.reader.isSet()) {
        if (nextField == null) {
          errStatus = true;
          throw new IOException("Statistics writer encountered unexpected field");
        }
        if (nextField.equals(Statistic.HLL)
            || nextField.equals(Statistic.HLL_MERGE)) {
          // Do NOT write out the HLL output, since it is not used yet for computing statistics for a
          // subset of partitions in the query OR for computing NDV with incremental statistics.
        }  else if (nextField.equals(Statistic.TDIGEST_MERGE)) {
          byte[] tdigest_bytearray = reader.readByteArray();
          ((DrillStatsTable.ColumnStatistics_v1) columnStatistics).buildHistogram(tdigest_bytearray);
        }
      }
    }

    @Override
    public void endField() throws IOException {
      nextField = null;
    }
  }

  @Override
  public FieldConverter getNewNullableFloat8Converter(int fieldId, String fieldName, FieldReader reader) {
    return new NullableFloat8JsonConverter(fieldId, fieldName, reader);
  }

  public class NullableFloat8JsonConverter extends FieldConverter {

    public NullableFloat8JsonConverter(int fieldId, String fieldName, FieldReader reader) {
      super(fieldId, fieldName, reader);
    }

    @Override
    public void startField() throws IOException {
      if (!skipNullFields || this.reader.isSet()) {
        if (fieldName.equals(Statistic.AVG_WIDTH)) {
          nextField = fieldName;
        }
      }
    }

    @Override
    public void writeField() throws IOException {
      if (!skipNullFields || this.reader.isSet()) {
        if (nextField == null) {
          errStatus = true;
          throw new IOException("Statistics writer encountered unexpected field");
        }
        if (nextField.equals(Statistic.AVG_WIDTH)) {
          ((DrillStatsTable.ColumnStatistics_v1) columnStatistics).setAvgWidth(reader.readDouble());
        }
      }
    }

    @Override
    public void endField() throws IOException {
      nextField = null;
    }
  }

  @Override
  public void startStatisticsRecord() throws IOException {
    columnStatistics = new DrillStatsTable.ColumnStatistics_v1();
  }

  @Override
  public void endStatisticsRecord() throws IOException {
    columnStatisticsList.add(columnStatistics);
    ++recordsWritten;
  }

  @Override
  public void flushBlockingWriter() throws IOException {
    Path permFileName = new Path(location, prefix + "." + extension);
    try {
      if (errStatus) {
        // Encountered some error
        throw new IOException("Statistics writer encountered unexpected field");
      } else if (recordsWritten < 0) {
        throw new IOException("Statistics writer did not have data");
      }
      // Generated the statistics data structure to be serialized
      statistics = DrillStatsTable.generateDirectoryStructure(dirComputedTime.toString(),
          columnStatisticsList);
      if (formatPlugin.supportsStatistics()) {
        // Invoke the format plugin stats API to write out the stats
        formatPlugin.writeStatistics(statistics, fs, fileName);
        // Delete existing permanent file and rename .tmp file to permanent file
        // If failed to do so then delete the .tmp file
        fs.delete(permFileName, false);
        fs.rename(fileName, permFileName);
        // Cancel delete once perm file is created
        fs.cancelDeleteOnExit(fileName);
        fs.cancelDeleteOnExit(new Path(location));
      }
      logger.debug("Created file: {}", permFileName);
    } catch(IOException ex) {
      logger.error("Unable to create file: " + permFileName, ex);
      throw ex;
    }
  }

  @Override
  public void abort() throws IOException {
    // Invoke cleanup to clear any .tmp files and/or empty statistics directory
    cleanup();
  }

  @Override
  public void cleanup() throws IOException {
    Path permFileName = new Path(location, prefix + "." + extension);
    try {
      // Remove the .tmp file, if any
      if (fs.exists(fileName)) {
        fs.delete(fileName, false);
        logger.debug("Deleted file: {}", fileName);
      }
      // Also delete the .stats.drill directory if no permanent file exists.
      if (!fs.exists(permFileName)) {
        fs.delete(new Path(location), false);
        logger.debug("Deleted directory: {}", location);
      }
    } catch (IOException ex) {
      logger.error("Unable to delete tmp file: " + fileName, ex);
      throw ex;
    }
  }
}
