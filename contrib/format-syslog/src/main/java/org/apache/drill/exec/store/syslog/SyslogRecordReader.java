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

package org.apache.drill.exec.store.syslog;

import io.netty.buffer.DrillBuf;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.exception.OutOfMemoryException;
import org.apache.drill.exec.expr.holders.VarCharHolder;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.store.AbstractRecordReader;
import org.apache.drill.exec.store.dfs.DrillFileSystem;
import org.apache.drill.exec.store.dfs.easy.FileWork;
import org.apache.drill.exec.vector.complex.impl.VectorContainerWriter;
import org.apache.drill.exec.vector.complex.writer.BaseWriter;
import org.apache.hadoop.fs.Path;
import org.realityforge.jsyslog.message.StructuredDataParameter;
import org.realityforge.jsyslog.message.SyslogMessage;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Map;
import java.util.Iterator;

public class SyslogRecordReader extends AbstractRecordReader {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SyslogRecordReader.class);
  private static final int MAX_RECORDS_PER_BATCH = 4096;

  private final DrillFileSystem fileSystem;
  private final FileWork fileWork;
  private final String userName;
  private BufferedReader reader;
  private DrillBuf buffer;
  private VectorContainerWriter writer;
  private SyslogFormatConfig config;
  private int maxErrors;
  private boolean flattenStructuredData;
  private int errorCount;
  private int lineCount;
  private List<SchemaPath> projectedColumns;
  private String line;

  private SimpleDateFormat df;

  public SyslogRecordReader(FragmentContext context,
                            DrillFileSystem fileSystem,
                            FileWork fileWork,
                            List<SchemaPath> columns,
                            String userName,
                            SyslogFormatConfig config) throws OutOfMemoryException {

    this.fileSystem = fileSystem;
    this.fileWork = fileWork;
    this.userName = userName;
    this.config = config;
    this.maxErrors = config.getMaxErrors();
    this.df = getValidDateObject("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
    this.errorCount = 0;
    this.buffer = context.getManagedBuffer().reallocIfNeeded(4096);
    this.projectedColumns = columns;
    this.flattenStructuredData = config.getFlattenStructuredData();

    setColumns(columns);
  }

  @Override
  public void setup(final OperatorContext context, final OutputMutator output) throws ExecutionSetupException {
    openFile();
    this.writer = new VectorContainerWriter(output);
  }

  private void openFile() {
    InputStream in;
    try {
      in = fileSystem.open(new Path(fileWork.getPath()));
    } catch (Exception e) {
      throw UserException
              .dataReadError(e)
              .message("Failed to open open input file: %s", fileWork.getPath())
              .addContext("User name", this.userName)
              .build(logger);
    }
    this.lineCount = 0;
    reader = new BufferedReader(new InputStreamReader(in));
  }

  @Override
  public int next() {
    this.writer.allocate();
    this.writer.reset();

    int recordCount = 0;

    try {
      BaseWriter.MapWriter map = this.writer.rootAsMap();
      String line = null;

      while (recordCount < MAX_RECORDS_PER_BATCH && (line = this.reader.readLine()) != null) {
        lineCount++;

        // Skip empty lines
        line = line.trim();
        if (line.length() == 0) {
          continue;
        }
        this.line = line;

        try {
          SyslogMessage parsedMessage = SyslogMessage.parseStructuredSyslogMessage(line);

          this.writer.setPosition(recordCount);
          map.start();

          if (isStarQuery()) {
            writeAllColumns(map, parsedMessage);
          } else {
            writeProjectedColumns(map, parsedMessage);
          }
          map.end();
          recordCount++;

        } catch (Exception e) {
          errorCount++;
          if (errorCount > maxErrors) {
            throw UserException
                    .dataReadError()
                    .message("Maximum Error Threshold Exceeded: ")
                    .addContext("Line: " + lineCount)
                    .addContext(e.getMessage())
                    .build(logger);
          }
        }
      }

      this.writer.setValueCount(recordCount);
      return recordCount;

    } catch (final Exception e) {
      errorCount++;
      if (errorCount > maxErrors) {
        throw UserException.dataReadError()
                .message("Error parsing file")
                .addContext(e.getMessage())
                .build(logger);
      }
    }

    return recordCount;
  }

  private void writeAllColumns(BaseWriter.MapWriter map, SyslogMessage parsedMessage) {

    long milliseconds = 0;
    try {
      milliseconds = parsedMessage.getTimestamp().getMillis();
    } catch (final Exception e) {
      errorCount++;
      if (errorCount > maxErrors) {
        throw UserException.dataReadError()
                .message("Syslog Format Plugin: Error parsing date")
                .addContext(e.getMessage())
                .build(logger);
      }
    }
    map.timeStamp("event_date").writeTimeStamp(milliseconds);
    map.integer("severity_code").writeInt(parsedMessage.getLevel().ordinal());
    map.integer("facility_code").writeInt(parsedMessage.getFacility().ordinal());

    mapStringField("severity", parsedMessage.getLevel().name(), map);
    mapStringField("facility", parsedMessage.getFacility().name(), map);
    mapStringField("ip", parsedMessage.getHostname(), map);
    mapStringField("app_name", parsedMessage.getAppName(), map);
    mapStringField("process_id", parsedMessage.getProcId(), map);
    mapStringField("message_id", parsedMessage.getMsgId(), map);

    if (parsedMessage.getStructuredData() != null) {
      mapStringField("structured_data_text", parsedMessage.getStructuredData().toString(), map);
      Map<String, List<StructuredDataParameter>> structuredData = parsedMessage.getStructuredData();
      if (flattenStructuredData) {
        mapFlattenedStructuredData(structuredData, map);
      } else {
        mapComplexField("structured_data", structuredData, map);
      }
    }
    mapStringField("message", parsedMessage.getMessage(), map);
  }

  private void writeProjectedColumns(BaseWriter.MapWriter map, SyslogMessage parsedMessage) throws UserException {
    String columnName;

    for (SchemaPath col : projectedColumns) {

      //Case for nested fields
      if (col.getAsNamePart().hasChild()) {
        String fieldName = col.getAsNamePart().getChild().getName();
        mapStructuredDataField(fieldName, map, parsedMessage);
      } else {
        columnName = col.getAsNamePart().getName();

        //Extracts fields from structured data IF the user selected to flatten these fields
        if ((!columnName.equals("structured_data_text")) && columnName.startsWith("structured_data_")) {
          String fieldName = columnName.replace("structured_data_", "");
          String value = getFieldFromStructuredData(fieldName, parsedMessage);
          mapStringField(columnName, value, map);
        } else {
          switch (columnName) {
            case "event_date":
              long milliseconds = parsedMessage.getTimestamp().getMillis(); //TODO put in try/catch
              map.timeStamp("event_date").writeTimeStamp(milliseconds);
              break;
            case "severity_code":
              map.integer("severity_code").writeInt(parsedMessage.getLevel().ordinal());
              break;
            case "facility_code":
              map.integer("facility_code").writeInt(parsedMessage.getFacility().ordinal());
              break;
            case "severity":
              mapStringField("severity", parsedMessage.getLevel().name(), map);
              break;
            case "facility":
              mapStringField("facility", parsedMessage.getFacility().name(), map);
              break;
            case "ip":
              mapStringField("ip", parsedMessage.getHostname(), map);
              break;
            case "app_name":
              mapStringField("app_name", parsedMessage.getAppName(), map);
              break;
            case "process_id":
              mapStringField("process_id", parsedMessage.getProcId(), map);
              break;
            case "msg_id":
              mapStringField("message_id", parsedMessage.getMsgId(), map);
              break;
            case "structured_data":
              if (parsedMessage.getStructuredData() != null) {
                Map<String, List<StructuredDataParameter>> structured_data = parsedMessage.getStructuredData();
                mapComplexField("structured_data", structured_data, map);
              }
              break;
            case "structured_data_text":
              if (parsedMessage.getStructuredData() != null) {
                mapStringField("structured_data_text", parsedMessage.getStructuredData().toString(), map);
              } else {
                mapStringField("structured_data_text", "", map);
              }
              break;
            case "message":
              mapStringField("message", parsedMessage.getMessage(), map);
              break;
            case "_raw":
              mapStringField("_raw", this.line, map);
              break;

            default:
              mapStringField(columnName, "", map);
          }
        }
      }
    }
  }

  //Helper function to map strings
  private void mapStringField(String name, String value, BaseWriter.MapWriter map) {
    if (value == null) {
      return;
    }
    try {
      byte[] bytes = value.getBytes("UTF-8");
      int stringLength = bytes.length;
      this.buffer = buffer.reallocIfNeeded(stringLength);
      this.buffer.setBytes(0, bytes, 0, stringLength);
      map.varChar(name).writeVarChar(0, stringLength, buffer);
    } catch (Exception e) {
      throw UserException
              .dataWriteError()
              .addContext("Could not write string: ")
              .addContext(e.getMessage())
              .build(logger);
    }
  }

  //Helper function to flatten structured data
  private void mapFlattenedStructuredData(Map<String, List<StructuredDataParameter>> data, BaseWriter.MapWriter map) {
    Iterator<Map.Entry<String, List<StructuredDataParameter>>> entries = data.entrySet().iterator();
    while (entries.hasNext()) {
      Map.Entry<String, List<StructuredDataParameter>> entry = entries.next();

      List<StructuredDataParameter> dataParameters = entry.getValue();
      String fieldName;
      String fieldValue;

      for (StructuredDataParameter parameter : dataParameters) {
        fieldName = "structured_data_" + parameter.getName();
        fieldValue = parameter.getValue();

        mapStringField(fieldName, fieldValue, map);
      }
    }
  }

  //Gets field from the Structured Data Construct
  private String getFieldFromStructuredData(String fieldName, SyslogMessage parsedMessage) {
    String result = null;
    Map<String, List<StructuredDataParameter>> structuredData = parsedMessage.getStructuredData();
    Iterator<Map.Entry<String, List<StructuredDataParameter>>> entries = parsedMessage.getStructuredData().entrySet().iterator();
    while (entries.hasNext()) {
      Map.Entry<String, List<StructuredDataParameter>> entry = entries.next();
      List<StructuredDataParameter> dataParameters = entry.getValue();

      for (StructuredDataParameter d : dataParameters) {
        if (d.getName().equals(fieldName)) {
          return d.getValue();
        }
      }
    }
    return result;
  }

  //Helper function to map arrays
  private void mapComplexField(String mapName, Map<String, List<StructuredDataParameter>> data, BaseWriter.MapWriter map) {
    Iterator<Map.Entry<String, List<StructuredDataParameter>>> entries = data.entrySet().iterator();
    while (entries.hasNext()) {
      Map.Entry<String, List<StructuredDataParameter>> entry = entries.next();

      List<StructuredDataParameter> dataParameters = entry.getValue();
      String fieldName;
      String fieldValue;

      for (StructuredDataParameter parameter : dataParameters) {
        fieldName = parameter.getName();
        fieldValue = parameter.getValue();

        VarCharHolder rowHolder = new VarCharHolder();

        byte[] rowStringBytes = fieldValue.getBytes();
        this.buffer.reallocIfNeeded(rowStringBytes.length);
        this.buffer.setBytes(0, rowStringBytes);
        rowHolder.start = 0;
        rowHolder.end = rowStringBytes.length;
        rowHolder.buffer = this.buffer;

        map.map(mapName).varChar(fieldName).write(rowHolder);
      }
    }
  }

  private void mapStructuredDataField(String fieldName, BaseWriter.MapWriter map, SyslogMessage parsedMessage) {
    String fieldValue = getFieldFromStructuredData(fieldName, parsedMessage);
    VarCharHolder rowHolder = new VarCharHolder();

    byte[] rowStringBytes = fieldValue.getBytes();
    this.buffer.reallocIfNeeded(rowStringBytes.length);
    this.buffer.setBytes(0, rowStringBytes);
    rowHolder.start = 0;
    rowHolder.end = rowStringBytes.length;
    rowHolder.buffer = this.buffer;

    map.map("structured_data").varChar(fieldName).write(rowHolder);
  }

  public SimpleDateFormat getValidDateObject(String d) {
    SimpleDateFormat tempDateFormat;
    if (d != null && !d.isEmpty()) {
      tempDateFormat = new SimpleDateFormat(d);
    } else {
      throw UserException
              .parseError()
              .message("Invalid date format")
              .build(logger);
    }
    return tempDateFormat;
  }

  public void close() throws Exception {
    this.reader.close();
  }
}
