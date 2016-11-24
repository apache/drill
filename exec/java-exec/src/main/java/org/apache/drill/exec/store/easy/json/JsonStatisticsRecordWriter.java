/**
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
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.util.MinimalPrettyPrinter;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.drill.exec.planner.common.DrillStatsTable;
import org.apache.drill.exec.record.VectorAccessible;
import org.apache.drill.exec.store.EventBasedRecordWriter.FieldConverter;
import org.apache.drill.exec.store.JSONOutputRecordWriter;
import org.apache.drill.exec.store.RecordWriter;
import org.apache.drill.exec.util.ImpersonationUtil;
import org.apache.drill.exec.vector.complex.reader.FieldReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.joda.time.DateTime;

public class JsonStatisticsRecordWriter extends JSONOutputRecordWriter implements RecordWriter {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(JsonStatisticsRecordWriter.class);
  private static final String LINE_FEED = String.format("%n");

  private String location;
  private String prefix;
  private String fieldDelimiter;
  private String extension;
  private boolean useExtendedOutput;
  private JsonGenerator generator;
  private FileSystem fs = null;
  private FSDataOutputStream stream = null;
  private int statisticsVersion;
  private final JsonFactory factory = new JsonFactory();
  private String lastDirectory = null;
  private String nextField = null;
  private DrillStatsTable.TableStatistics statistics;
  private List<DrillStatsTable.ColumnStatistics> columnStatisticsList = new ArrayList<DrillStatsTable.ColumnStatistics>();
  private DrillStatsTable.ColumnStatistics columnStatistics;
  private String dirComputedTime = null;
  private Path fileName = null;
  private String queryId = null;
  private long recordsWritten = -1;
  private boolean errStatus = false;

  public JsonStatisticsRecordWriter(){
  }

  @Override
  public void init(Map<String, String> writerOptions) throws IOException {
    this.location = writerOptions.get("location");
    this.prefix = writerOptions.get("prefix");
    this.fieldDelimiter = writerOptions.get("separator");
    this.extension = writerOptions.get("extension");
    this.useExtendedOutput = Boolean.parseBoolean(writerOptions.get("extended"));
    this.skipNullFields = Boolean.parseBoolean(writerOptions.get("skipnulls"));
    final boolean uglify = Boolean.parseBoolean(writerOptions.get("uglify"));
    this.statisticsVersion = (int)Long.parseLong(writerOptions.get("statsversion"));
    this.queryId = writerOptions.get("queryid");
    Configuration conf = new Configuration();
    conf.set(FileSystem.FS_DEFAULT_NAME_KEY, writerOptions.get(FileSystem.FS_DEFAULT_NAME_KEY));
     //Write as DRILL process user
    this.fs = ImpersonationUtil.createFileSystem(ImpersonationUtil.getProcessUserName(), conf);

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
      stream = fs.create(fileName);
      generator = factory.createGenerator(stream).useDefaultPrettyPrinter();
      if (uglify) {
        generator = generator.setPrettyPrinter(new MinimalPrettyPrinter(LINE_FEED));
      }
      logger.debug("Created file: {}", fileName);
    } catch (IOException ex) {
      logger.error("Unable to create file: " + fileName, ex);
      generator.close();
      stream.close();
      throw ex;
    }
  }

  private boolean generateDirectoryStructure() {
    //TODO: Split up columnStatisticsList() based on directory names. We assume only
    //one directory right now but this WILL change in the future
    //HashMap<String, Boolean> dirNames = new HashMap<String, Boolean>();
    if (statisticsVersion == 0) {
      statistics = new DrillStatsTable.Statistics_v0();
      List<DrillStatsTable.DirectoryStatistics_v0> dirStats = new ArrayList<DrillStatsTable.DirectoryStatistics_v0>();

      //Create dirStats
      DrillStatsTable.DirectoryStatistics_v0 dirStat = new DrillStatsTable.DirectoryStatistics_v0();
      // value of PI
      dirStat.setComputedTime(3.1415926535897932);
      //Add this dirStats to the list of dirStats
      dirStats.add(dirStat);

      //Add list of dirStats to tableStats
      ((DrillStatsTable.Statistics_v0) statistics).setDirectoryStatistics(dirStats);

      return true;
    } else if (statisticsVersion == 1) {
      statistics = new DrillStatsTable.Statistics_v1();

      List<DrillStatsTable.DirectoryStatistics_v1> dirStats = new ArrayList<DrillStatsTable.DirectoryStatistics_v1>();
      List<DrillStatsTable.ColumnStatistics_v1> columnStatisticsV1s = new ArrayList<DrillStatsTable.ColumnStatistics_v1>();

      //Create dirStats
      DrillStatsTable.DirectoryStatistics_v1 dirStat = new DrillStatsTable.DirectoryStatistics_v1();
      // Add columnStats corresponding to this dirStats
      for (DrillStatsTable.ColumnStatistics colStats : columnStatisticsList) {
        columnStatisticsV1s.add(((DrillStatsTable.ColumnStatistics_v1) colStats));
      }

      dirStat.setComputedTime(dirComputedTime);
      dirStat.setColumnStatistics(columnStatisticsV1s);
      //Add this dirStats to the list of dirStats
      dirStats.add(dirStat);
      //Add list of dirStats to tableStats
      ((DrillStatsTable.Statistics_v1) statistics).setDirectoryStatistics(dirStats);

      return true;
    }
    return false;
  }

  @Override
  public void updateSchema(VectorAccessible batch) throws IOException {
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
      if (statisticsVersion == 1) {
        if (fieldName.equalsIgnoreCase("SCHEMA")) {
          nextField = fieldName;
        } else if (fieldName.equalsIgnoreCase("STATCOUNT")
              || fieldName.equalsIgnoreCase("NONNULLSTATCOUNT")
              || fieldName.equalsIgnoreCase("NDV")
              || fieldName.equalsIgnoreCase("AVG_WIDTH")) {
          nextField = fieldName;
        }
      }
    }

    @Override
    public void writeField() throws IOException {
      if (statisticsVersion == 1) {
        if (nextField == null) {
          errStatus = true;
          throw new IOException("Statistics writer encountered unexpected field");
        }
        if (nextField.equalsIgnoreCase("SCHEMA")) {
          ((DrillStatsTable.ColumnStatistics_v1) columnStatistics).setSchema(reader.readLong());
        } else if (nextField.equalsIgnoreCase("STATCOUNT")) {
          ((DrillStatsTable.ColumnStatistics_v1) columnStatistics).setCount(reader.readLong());
        } else if (nextField.equalsIgnoreCase("NONNULLSTATCOUNT")) {
          ((DrillStatsTable.ColumnStatistics_v1) columnStatistics).setNonNullCount(reader.readLong());
        } else if (nextField.equalsIgnoreCase("NDV")) {
          ((DrillStatsTable.ColumnStatistics_v1) columnStatistics).setNdv(reader.readLong());
        } else if (nextField.equalsIgnoreCase("AVG_WIDTH")) {
          ((DrillStatsTable.ColumnStatistics_v1) columnStatistics).setAvgWidth(reader.readLong());
        }
      }
    }

    @Override
    public void endField() throws IOException {
      if (statisticsVersion == 1) {
        nextField = null;
      }
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
      if (statisticsVersion == 1) {
        if (fieldName.equalsIgnoreCase("COMPUTED")) {
          nextField = fieldName;
        }
      }
    }

    @Override
    public void writeField() throws IOException {
      if (statisticsVersion == 1) {
        if (nextField == null) {
          errStatus = true;
          throw new IOException("Statistics writer encountered unexpected field");
        }
        if (nextField.equalsIgnoreCase(("COMPUTED"))) {
          String computedTime = reader.readDateTime().toString();
          if (dirComputedTime == null
                 || computedTime.compareTo(dirComputedTime) > 0) {
            dirComputedTime = computedTime;
          }
        }
      }
    }

    @Override
    public void endField() throws IOException {
      if (statisticsVersion == 1) {
        nextField = null;
      }
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
      if (statisticsVersion == 1) {
        if (fieldName.equalsIgnoreCase("COLUMN")) {
          nextField = fieldName;
        }
      }
    }

    @Override
    public void writeField() throws IOException {
      if (statisticsVersion == 1) {
        if (nextField == null) {
          errStatus = true;
          throw new IOException("Statistics writer encountered unexpected field");
        }
        if (nextField.equalsIgnoreCase("COLUMN")) {
          ((DrillStatsTable.ColumnStatistics_v1) columnStatistics).setName(reader.readText().toString());
        }
      }
    }

    @Override
    public void endField() throws IOException {
      if (statisticsVersion == 1) {
        nextField = null;
      }
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
      if (statisticsVersion == 1) {
        if (!skipNullFields || this.reader.isSet()) {
          if (fieldName.equalsIgnoreCase("STATCOUNT")
              || fieldName.equalsIgnoreCase("NONNULLSTATCOUNT")
              || fieldName.equalsIgnoreCase("NDV")) {
            nextField = fieldName;
          }
        }
      }
    }

    @Override
    public void writeField() throws IOException {
      if (statisticsVersion == 1) {
        if (!skipNullFields || this.reader.isSet()) {
          if (nextField == null) {
            errStatus = true;
            throw new IOException("Statistics writer encountered unexpected field");
          }
          if (nextField.equalsIgnoreCase("STATCOUNT")) {
            ((DrillStatsTable.ColumnStatistics_v1) columnStatistics).setCount(reader.readLong());
          } else if (nextField.equalsIgnoreCase("NONNULLSTATCOUNT")) {
            ((DrillStatsTable.ColumnStatistics_v1) columnStatistics).setNonNullCount(reader.readLong());
          } else if (nextField.equalsIgnoreCase("NDV")) {
            ((DrillStatsTable.ColumnStatistics_v1) columnStatistics).setNdv(reader.readLong());
          }
        }
      }
    }

    @Override
    public void endField() throws IOException {
      if (statisticsVersion == 1) {
        nextField = null;
      }
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
      if (statisticsVersion == 1) {
        if (!skipNullFields || this.reader.isSet()) {
          if (fieldName.equalsIgnoreCase("HLL")) {
            nextField = fieldName;
          } else if (fieldName.equalsIgnoreCase("HLL_MERGE")) {
            nextField = fieldName;
          }
        }
      }
    }

    @Override
    public void writeField() throws IOException {
      if (statisticsVersion == 1) {
        if (!skipNullFields || this.reader.isSet()) {
          if (nextField == null) {
            errStatus = true;
            throw new IOException("Statistics writer encountered unexpected field");
          }
          if (nextField.equalsIgnoreCase("HLL")) {
            //TODO: Write Hll output
            //((DrillStatsTable.ColumnStatistics_v1) columnStatistics).setHLL(reader.readByteArray());
          } else if (nextField.equalsIgnoreCase("HLL_MERGE")) {
            ((DrillStatsTable.ColumnStatistics_v1) columnStatistics).setHLL(reader.readByteArray());
          }
        }
      }
    }

    @Override
    public void endField() throws IOException {
      if (statisticsVersion == 1) {
        nextField = null;
      }
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
      if (statisticsVersion == 1) {
        if (!skipNullFields || this.reader.isSet()) {
          if (fieldName.equalsIgnoreCase("AVG_WIDTH")) {
            nextField = fieldName;
          }
        }
      }
    }

    @Override
    public void writeField() throws IOException {
      if (statisticsVersion == 1) {
        if (!skipNullFields || this.reader.isSet()) {
          if (nextField == null) {
            errStatus = true;
            throw new IOException("Statistics writer encountered unexpected field");
          }
          if (nextField.equalsIgnoreCase("AVG_WIDTH")) {
            ((DrillStatsTable.ColumnStatistics_v1) columnStatistics).setAvgWidth(reader.readDouble());
          }
        }
      }
    }

    @Override
    public void endField() throws IOException {
      if (statisticsVersion == 1) {
        nextField = null;
      }
    }
  }

  @Override
  public void startRecord() throws IOException {
    if (statisticsVersion == 1) {
      columnStatistics = new DrillStatsTable.ColumnStatistics_v1();
    }
  }

  @Override
  public void endRecord() throws IOException {
    if (statisticsVersion == 1) {
      columnStatisticsList.add(columnStatistics);
    }
    ++recordsWritten;
  }

  @Override
  public void abort() throws IOException {
  }

  @Override
  public void cleanup() throws IOException {

    Path permFileName = new Path(location, prefix + "." + extension);
    try {
      if (errStatus) {
        // Encountered some error
        throw new IOException("Statistics writer encountered unexpected field");
      } else if (recordsWritten < 0) {
        throw new IOException("Statistics writer did not have data");
      }

      if (generateDirectoryStructure()) {
        ObjectMapper mapper = new ObjectMapper();
        mapper.writeValue(generator, statistics);
      }
      generator.flush();
      stream.close();

      // Delete existing permanent file and rename .tmp file to permanent file
      // If failed to do so then delete the .tmp file
      fs.delete(permFileName, false);
      fs.rename(fileName, permFileName);
      logger.debug("Created file: {}", permFileName);
    } catch (com.fasterxml.jackson.core.JsonGenerationException ex) {
      logger.error("Unable to create file (JSON generation error): " + permFileName, ex);
      throw ex;
    } catch (com.fasterxml.jackson.databind.JsonMappingException ex) {
      logger.error("Unable to create file (JSON mapping error): " + permFileName, ex);
      throw ex;
    } catch(IOException ex) {
      logger.error("Unable to create file: " + permFileName, ex);
      throw ex;
    } finally {
      try {
        // Remove the .tmp file
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
}
