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

package org.apache.drill.exec.store.msaccess;

import com.bedatadriven.msaccess.MsaccessDataFileReader;
import com.bedatadriven.msaccess.MsaccessVariable;
import org.apache.drill.common.AutoCloseables;
import org.apache.drill.common.exceptions.CustomErrorContext;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.exec.physical.impl.scan.file.FileScanFramework;
import org.apache.drill.exec.physical.impl.scan.framework.ManagedReader;
import org.apache.drill.exec.physical.resultSet.ResultSetLoader;
import org.apache.drill.exec.physical.resultSet.RowSetLoader;
import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.vector.accessor.ScalarWriter;
import org.apache.hadoop.mapred.FileSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class MsaccessBatchReader implements ManagedReader<FileScanFramework.FileSchemaNegotiator> {

  private static final Logger logger = LoggerFactory.getLogger(MsaccessBatchReader.class);

  private static final String VALUE_LABEL = "_value";

  private final int maxRecords;

  private FileSplit split;

  private InputStream fsStream;

  private MsaccessDataFileReader msaccessReader;

  private RowSetLoader rowWriter;

  private List<MsaccessVariable> variableList;

  private List<MsaccessColumnWriter> writerList;

  private CustomErrorContext errorContext;


  public static class MsaccessReaderConfig {

    protected final MsaccessFormatPlugin plugin;

    public MsaccessReaderConfig(MsaccessFormatPlugin plugin) {
      this.plugin = plugin;
    }
  }

  public MsaccessBatchReader(int maxRecords) {
    this.maxRecords = maxRecords;
  }

  @Override
  public boolean open(FileScanFramework.FileSchemaNegotiator negotiator) {
    split = negotiator.split();
    openFile(negotiator);
    negotiator.tableSchema(buildSchema(), true);
    errorContext = negotiator.parentErrorContext();
    ResultSetLoader loader = negotiator.build();
    rowWriter = loader.writer();
    buildReaderList();

    return true;
  }

  @Override
  public boolean next() {
    while (!rowWriter.isFull()) {
      if (!processNextRow()) {
        return false;
      }
    }
    return true;
  }

  @Override
  public void close() {
    if (fsStream != null) {
      AutoCloseables.closeSilently(fsStream);
      fsStream = null;
    }
  }

  private void openFile(FileSchemaNegotiator negotiator) {
    try {
      fsStream = negotiator.fileSystem().openPossiblyCompressedStream(split.getPath());
      msaccessReader = new MsaccessDataFileReader(fsStream);
    } catch (IOException e) {
      throw UserException
        .dataReadError(e)
        .message("Unable to open MSAccess File %s", split.getPath())
        .addContext(e.getMessage())
        .addContext(errorContext)
        .build(logger);
    }
  }

  private boolean processNextRow() {
    // Check to see if the limit has been reached
    if (rowWriter.limitReached(maxRecords)) {
      return false;
    }

    try {
      // Stop reading when you run out of data
      if (!msaccessReader.readNextCase()) {
        return false;
      }

      rowWriter.start();
      for (MsaccessColumnWriter msaccessColumnWriter : writerList) {
        msaccessColumnWriter.load(msaccessReader);
      }
      rowWriter.save();

    } catch (IOException e) {
      throw UserException.dataReadError(e).message("Error reading MSAccess File.").addContext(errorContext).build(logger);
    }
    return true;
  }

  private TupleMetadata buildSchema() {
    SchemaBuilder builder = new SchemaBuilder();
    variableList = msaccessReader.getVariables();

    for (MsaccessVariable variable : variableList) {
      String varName = variable.getVariableName();

      if (variable.isNumeric()) {
        builder.addNullable(varName, TypeProtos.MinorType.FLOAT8);

        // Check if the column has lookups associated with it
        if (variable.getValueLabels() != null && variable.getValueLabels().size() > 0) {
          builder.addNullable(varName + VALUE_LABEL, TypeProtos.MinorType.VARCHAR);
        }

      } else {
        builder.addNullable(varName, TypeProtos.MinorType.VARCHAR);
      }
    }
    return builder.buildSchema();
  }

  private void buildReaderList() {
    writerList = new ArrayList<>();

    for (MsaccessVariable variable : variableList) {
      if (variable.isNumeric()) {
        writerList.add(new NumericMsaccessColumnWriter(variable.getIndex(), variable.getVariableName(), rowWriter, msaccessReader));
      } else {
        writerList.add(new StringMsaccessColumnWriter(variable.getIndex(), variable.getVariableName(), rowWriter));
      }
    }
  }

  public abstract static class MsaccessColumnWriter {
    final String columnName;

    final ScalarWriter writer;

    final int columnIndex;

    public MsaccessColumnWriter(int columnIndex, String columnName, ScalarWriter writer) {
      this.columnIndex = columnIndex;
      this.columnName = columnName;
      this.writer = writer;
    }

    public abstract void load(MsaccessDataFileReader reader);
  }

  public static class StringMsaccessColumnWriter extends MsaccessColumnWriter {

    StringMsaccessColumnWriter(int columnIndex, String columnName, RowSetLoader rowWriter) {
      super(columnIndex, columnName, rowWriter.scalar(columnName));
    }

    @Override
    public void load(MsaccessDataFileReader reader) {
      writer.setString(reader.getStringValue(columnIndex));
    }
  }

  public static class NumericMsaccessColumnWriter extends MsaccessColumnWriter {

    ScalarWriter labelWriter;

    Map<Double, String> labels;

    NumericMsaccessColumnWriter(int columnIndex, String columnName, RowSetLoader rowWriter, MsaccessDataFileReader reader) {
      super(columnIndex, columnName, rowWriter.scalar(columnName));

      if (reader.getValueLabels(columnName) != null && reader.getValueLabels(columnName).size() != 0) {
        labelWriter = rowWriter.scalar(columnName + VALUE_LABEL);
        labels = reader.getValueLabels(columnIndex);
      }
    }

    @Override
    public void load(MsaccessDataFileReader reader) {
      double value = reader.getDoubleValue(columnIndex);

      if (labelWriter != null) {
        String labelValue = labels.get(value);
        if (labelValue == null) {
          labelWriter.setNull();
        } else {
          labelWriter.setString(labelValue);
        }
      }
      writer.setDouble(value);
    }
  }
}


