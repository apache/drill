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
package org.apache.drill.exec.store.log;

import org.apache.drill.shaded.guava.com.google.common.base.Charsets;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.store.AbstractRecordReader;
import org.apache.drill.exec.store.dfs.DrillFileSystem;
import org.apache.drill.exec.store.dfs.easy.FileWork;
import org.apache.drill.exec.vector.NullableIntVector;
import org.apache.drill.exec.vector.NullableBigIntVector;
import org.apache.drill.exec.vector.NullableSmallIntVector;
import org.apache.drill.exec.vector.NullableFloat4Vector;
import org.apache.drill.exec.vector.NullableFloat8Vector;
import org.apache.drill.exec.vector.BaseValueVector;
import org.apache.drill.exec.vector.NullableDateVector;
import org.apache.drill.exec.vector.NullableVarCharVector;
import org.apache.drill.exec.vector.NullableTimeStampVector;
import org.apache.drill.exec.vector.NullableTimeVector;


import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

public class LogRecordReader extends AbstractRecordReader {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(LogRecordReader.class);

  private abstract static class ColumnDefn {
    private final String name;
    private final int index;
    private final String format;

    public ColumnDefn(String name, int index) {
      this(name, index, null);
    }

    public ColumnDefn(String name, int index, String format) {
      this.name = name;
      this.index = index;
      this.format = format;
    }

    public abstract void define(OutputMutator outputMutator) throws SchemaChangeException;

    public abstract void load(int rowIndex, String value);

    public String getName() { return this.name; }

    public int getIndex() { return this.index; }

    public String getFormat() { return this.format;}

    @Override
    //For testing
    public String toString() {
      return "Name: " + name + ", Index: " + index + ", Format: " + format;
    }
  }

  private static class VarCharDefn extends ColumnDefn {

    private NullableVarCharVector.Mutator mutator;

    public VarCharDefn(String name, int index) {
      super(name, index);
    }

    @Override
    public void define(OutputMutator outputMutator) throws SchemaChangeException {
      MaterializedField field = MaterializedField.create(getName(),
          Types.optional(MinorType.VARCHAR));
      mutator = outputMutator.addField(field, NullableVarCharVector.class).getMutator();
    }

    @Override
    public void load(int rowIndex, String value) {
      mutator.set(rowIndex, value.getBytes());
    }
  }

  private static class BigIntDefn extends ColumnDefn {

    private NullableBigIntVector.Mutator mutator;

    public BigIntDefn(String name, int index) {
      super(name, index);
    }

    @Override
    public void define(OutputMutator outputMutator) throws SchemaChangeException {
      MaterializedField field = MaterializedField.create(getName(),
          Types.optional(MinorType.BIGINT));
      mutator = outputMutator.addField(field, NullableBigIntVector.class).getMutator();
    }

    @Override
    public void load(int rowIndex, String value) {
      try {
        mutator.set(rowIndex, Long.parseLong(value));
      } catch (NumberFormatException e) {
        throw UserException
            .dataReadError(e)
            .addContext("Failed to parse an INT field")
            .addContext("Column", getName())
            .addContext("Position", getIndex())
            .addContext("Value", value)
            .build(logger);
      }
    }
  }

  private static class SmallIntDefn extends ColumnDefn {

    private NullableSmallIntVector.Mutator mutator;

    public SmallIntDefn(String name, int index) {
      super(name, index);
    }

    @Override
    public void define(OutputMutator outputMutator) throws SchemaChangeException {
      MaterializedField field = MaterializedField.create(getName(),
          Types.optional(MinorType.SMALLINT));
      mutator = outputMutator.addField(field, NullableSmallIntVector.class).getMutator();
    }

    @Override
    public void load(int rowIndex, String value) {
      try {
        mutator.set(rowIndex, Short.parseShort(value));
      } catch (NumberFormatException e) {
        throw UserException
            .dataReadError(e)
            .addContext("Failed to parse an INT field")
            .addContext("Column", getName())
            .addContext("Position", getIndex())
            .addContext("Value", value)
            .build(logger);
      }
    }
  }

  private static class IntDefn extends ColumnDefn {

    private NullableIntVector.Mutator mutator;

    public IntDefn(String name, int index) {
      super(name, index);
    }

    @Override
    public void define(OutputMutator outputMutator) throws SchemaChangeException {
      MaterializedField field = MaterializedField.create(getName(),
          Types.optional(MinorType.INT));
      mutator = outputMutator.addField(field, NullableIntVector.class).getMutator();
    }

    @Override
    public void load(int rowIndex, String value) {
      try {
        mutator.set(rowIndex, Integer.parseInt(value));
      } catch (NumberFormatException e) {
        throw UserException
            .dataReadError(e)
            .addContext("Failed to parse an INT field")
            .addContext("Column", getName())
            .addContext("Position", getIndex())
            .addContext("Value", value)
            .build(logger);
      }
    }
  }

  private static class Float4Defn extends ColumnDefn {

    private NullableFloat4Vector.Mutator mutator;

    public Float4Defn(String name, int index) {
      super(name, index);
    }

    @Override
    public void define(OutputMutator outputMutator) throws SchemaChangeException {
      MaterializedField field = MaterializedField.create(getName(),
          Types.optional(MinorType.FLOAT4));
      mutator = outputMutator.addField(field, NullableFloat4Vector.class).getMutator();
    }

    @Override
    public void load(int rowIndex, String value) {
      try {
        mutator.set(rowIndex, Float.parseFloat(value));
      } catch (NumberFormatException e) {
        throw UserException
            .dataReadError(e)
            .addContext("Failed to parse an FLOAT field")
            .addContext("Column", getName())
            .addContext("Position", getIndex())
            .addContext("Value", value)
            .build(logger);
      }
    }
  }

  private static class DoubleDefn extends ColumnDefn {

    private NullableFloat8Vector.Mutator mutator;

    public DoubleDefn(String name, int index) {
      super(name, index);
    }

    @Override
    public void define(OutputMutator outputMutator) throws SchemaChangeException {
      MaterializedField field = MaterializedField.create(getName(),
          Types.optional(MinorType.FLOAT8));
      mutator = outputMutator.addField(field, NullableFloat8Vector.class).getMutator();
    }

    @Override
    public void load(int rowIndex, String value) {
      try {
        mutator.set(rowIndex, Double.parseDouble(value));
      } catch (NumberFormatException e) {
        throw UserException
            .dataReadError(e)
            .addContext("Failed to parse an FLOAT field")
            .addContext("Column", getName())
            .addContext("Position", getIndex())
            .addContext("Value", value)
            .build(logger);
      }
    }
  }

  private static class DateDefn extends ColumnDefn {

    private NullableDateVector.Mutator mutator;
    private SimpleDateFormat df;

    public DateDefn(String name, int index, String dateFormat) {
      super(name, index, dateFormat);
      df = getValidDateObject(dateFormat);
    }

    private SimpleDateFormat getValidDateObject(String d) {
      SimpleDateFormat tempDateFormat;
      if (d != null && !d.isEmpty()) {
        tempDateFormat = new SimpleDateFormat(d);
      } else {
        throw UserException.parseError()
            .message("Invalid date format.  The date formatting string was empty.")
            .build(logger);
      }
      return tempDateFormat;
    }

    @Override
    public void define(OutputMutator outputMutator) throws SchemaChangeException {
      MaterializedField field = MaterializedField.create(getName(),
          Types.optional(MinorType.DATE));
      mutator = outputMutator.addField(field, NullableDateVector.class).getMutator();
    }

    @Override
    public void load(int rowIndex, String value) {
      try {
        Date d = df.parse(value);
        long milliseconds = d.getTime();
        mutator.set(rowIndex, milliseconds);
      } catch (NumberFormatException e) {
        throw UserException
            .dataReadError(e)
            .addContext("Failed to parse an DATE field")
            .addContext("Column", getName())
            .addContext("Position", getIndex())
            .addContext("Value", value)
            .build(logger);
      } catch (ParseException e) {
        throw UserException
            .dataReadError(e)
            .addContext("Date Format String does not match field value.")
            .addContext("Column", getName())
            .addContext("Position", getIndex())
            .addContext("Format String", getFormat())
            .addContext("Value", value)
            .build(logger);
      }
    }
  }

  private static class TimeDefn extends ColumnDefn {

    private NullableTimeVector.Mutator mutator;
    private SimpleDateFormat df;

    public TimeDefn(String name, int index, String dateFormat) {
      super(name, index, dateFormat);
      df = getValidDateObject(dateFormat);
    }

    private SimpleDateFormat getValidDateObject(String d) {
      SimpleDateFormat tempDateFormat;
      if (d != null && !d.isEmpty()) {
        tempDateFormat = new SimpleDateFormat(d);
      } else {
        throw UserException.parseError()
            .message("Invalid date format.  The date formatting string was empty.")
            .build(logger);
      }
      return tempDateFormat;
    }

    @Override
    public void define(OutputMutator outputMutator) throws SchemaChangeException {
      MaterializedField field = MaterializedField.create(getName(),
          Types.optional(MinorType.TIME));
      mutator = outputMutator.addField(field, NullableTimeVector.class).getMutator();
    }

    @Override
    public void load(int rowIndex, String value) {
      try {
        Date d = df.parse(value);
        int milliseconds = (int) d.getTime();
        mutator.set(rowIndex, milliseconds);
      } catch (NumberFormatException e) {
        throw UserException
            .dataReadError(e)
            .addContext("Failed to parse an Time field")
            .addContext("Column", getName())
            .addContext("Position", getIndex())
            .addContext("Value", value)
            .build(logger);
      } catch (ParseException e) {
        throw UserException
            .dataReadError(e)
            .addContext("Date Format String does not match field value.")
            .addContext("Column", getName())
            .addContext("Position", getIndex())
            .addContext("Format String", getFormat())
            .addContext("Value", value)
            .build(logger);
      }
    }
  }

  private static class TimeStampDefn extends ColumnDefn {

    private NullableTimeStampVector.Mutator mutator;
    private SimpleDateFormat df;

    public TimeStampDefn(String name, int index, String dateFormat) {
      super(name, index, dateFormat);
      df = getValidDateObject(dateFormat);
    }

    private SimpleDateFormat getValidDateObject(String d) {
      SimpleDateFormat tempDateFormat;
      if (d != null && !d.isEmpty()) {
        tempDateFormat = new SimpleDateFormat(d);
      } else {
        throw UserException.parseError()
            .message("Invalid date format.  The date formatting string was empty.")
            .build(logger);
      }
      return tempDateFormat;
    }

    @Override
    public void define(OutputMutator outputMutator) throws SchemaChangeException {
      MaterializedField field = MaterializedField.create(getName(),
          Types.optional(MinorType.TIMESTAMP));
      mutator = outputMutator.addField(field, NullableTimeStampVector.class).getMutator();
    }

    @Override
    public void load(int rowIndex, String value) {
      try {
        Date d = df.parse(value);
        long milliseconds = d.getTime();
        mutator.set(rowIndex, milliseconds);
      } catch (NumberFormatException e) {
        throw UserException
            .dataReadError(e)
            .addContext("Failed to parse a TIMESTAMP field")
            .addContext("Column", getName())
            .addContext("Position", getIndex())
            .addContext("Value", value)
            .build(logger);
      } catch (ParseException e) {
        throw UserException
            .dataReadError(e)
            .addContext("Date Format String does not match field value.")
            .addContext("Column", getName())
            .addContext("Position", getIndex())
            .addContext("Format String", getFormat())
            .addContext("Value", value)
            .build(logger);
      }
    }
  }

  private static final int BATCH_SIZE = BaseValueVector.INITIAL_VALUE_ALLOCATION;

  private final DrillFileSystem dfs;
  private final FileWork fileWork;
  private final String userName;
  private final LogFormatConfig formatConfig;
  private ColumnDefn columns[];
  private Pattern pattern;
  private BufferedReader reader;
  private int rowIndex;
  private int capturingGroups;
  private OutputMutator outputMutator;
  private int unmatchedColumnIndex;
  private int unmatchedRowIndex;
  private boolean unmatchedRows;
  private int maxErrors;


  private int errorCount;


  public LogRecordReader(FragmentContext context, DrillFileSystem dfs,
                         FileWork fileWork, List<SchemaPath> columns, String userName,
                         LogFormatConfig formatConfig) {
    this.dfs = dfs;
    this.fileWork = fileWork;
    this.userName = userName;
    this.formatConfig = formatConfig;
    this.unmatchedColumnIndex = -1;
    this.unmatchedRowIndex = 0;
    this.unmatchedRows = false;
    this.maxErrors = formatConfig.getMaxErrors();

    // Ask the superclass to parse the projection list.
    setColumns(columns);

    if (maxErrors < 0) {
      throw UserException
          .validationError()
          .message("Max Errors must be a positive integer greater than zero.")
          .build(logger);
    }

  }

  @Override
  public void setup(final OperatorContext context, final OutputMutator output) {
    this.outputMutator = output;

    setupPattern();
    openFile();
    setupProjection();
    defineVectors();
  }

  private void setupPattern() {
    try {
      this.pattern = Pattern.compile(this.formatConfig.getRegex());
      Matcher m = pattern.matcher("test");
      capturingGroups = m.groupCount();
    } catch (PatternSyntaxException e) {
      throw UserException
          .validationError(e)
          .message("Failed to parse regex: \"%s\"", formatConfig.getRegex())
          .build(logger);
    }
  }

  private void setupProjection() {
    if (isSkipQuery()) {
      projectNone();
    } else if (isStarQuery()) {
      projectAll();
    } else {
      projectSubset();
    }
  }

  private void projectNone() {
    columns = new ColumnDefn[]{new VarCharDefn("dummy", -1)};
  }

  private void openFile() {
    InputStream in;
    try {
      in = dfs.open(new Path(fileWork.getPath()));
    } catch (Exception e) {
      throw UserException
          .dataReadError(e)
          .message("Failed to open open input file: %s", fileWork.getPath())
          .addContext("User name", userName)
          .build(logger);
    }
    reader = new BufferedReader(new InputStreamReader(in, Charsets.UTF_8));
  }

  private void projectAll() {
    List<String> fields = formatConfig.getFieldNames();
    for (int i = fields.size(); i < capturingGroups; i++) {
      fields.add("field_" + i);
    }
    columns = new ColumnDefn[capturingGroups];

    for (int i = 0; i < capturingGroups; i++) {
      columns[i] = makeColumn(fields.get(i), i);
    }
  }

  private void projectSubset() {
    Collection<SchemaPath> project = this.getColumns();
    assert !project.isEmpty();
    columns = new ColumnDefn[project.size()];

    List<String> fields = formatConfig.getFieldNames();
    int colIndex = 0;


    for (SchemaPath column : project) {
      if (column.getAsNamePart().hasChild()) {
        throw UserException
            .validationError()
            .message("The log format plugin supports only simple columns")
            .addContext("Projected column", column.toString())
            .build(logger);
      }

      String name = column.getAsNamePart().getName();

      //Need this to retrieve unnamed fields
      Pattern r = Pattern.compile("^field_(\\d+)$");
      Matcher m = r.matcher(name);
      int patternIndex = -1;

      if (name.equals("_unmatched_rows")) {
        //Set boolean flag to true
        this.unmatchedRows = true;
        this.unmatchedColumnIndex = colIndex;
      } else if (m.find()) {
        //if no fields are defined in the configuration, then all the fields have names of 'field_n'
        //Therefore n is the column index
        patternIndex = Integer.parseInt(m.group(1));
      } else {
        for (int i = 0; i < fields.size(); i++) {
          if (fields.get(i).equalsIgnoreCase(name) ||
              fields.get(i).equals("_raw") ||
              fields.get(i).equals("_unmatched_rows")
              ) {
            patternIndex = i;

            break;
          }
        }
      }
      columns[colIndex++] = makeColumn(name, patternIndex);
    }

  }

  private ColumnDefn makeColumn(String name, int patternIndex) {
    String typeName = null;
    if (patternIndex <= -1 || formatConfig.getSchema() == null) {
      // Use VARCHAR for missing columns
      // (instead of Drill standard of nullable int)
      typeName = MinorType.VARCHAR.name();
    } else if (patternIndex < formatConfig.getSchema().size()) {
      //typeName = formatConfig.getDataType(patternIndex);
      LogFormatField tempField = formatConfig.getField(patternIndex);
      typeName = tempField.getFieldType().toUpperCase();
    }
    if (typeName == null) {
      // No type name. VARCHAR is a safe guess
      typeName = MinorType.VARCHAR.name();
    }
    if (name.equals("_raw") || name.equals("_unmatched_rows")) {
      return new VarCharDefn(name, patternIndex);
    }

    MinorType type = MinorType.valueOf(typeName);
    //System.out.println( "Type name: "  + typeName + " Type: " + type);
    switch (type) {
      case VARCHAR:
        return new VarCharDefn(name, patternIndex);
      case INT:
        return new IntDefn(name, patternIndex);
      case SMALLINT:
        return new SmallIntDefn(name, patternIndex);
      case BIGINT:
        return new BigIntDefn(name, patternIndex);
      case FLOAT4:
        return new Float4Defn(name, patternIndex);
      case FLOAT8:
        return new DoubleDefn(name, patternIndex);
      case DATE:
        return new DateDefn(name, patternIndex, formatConfig.getDateFormat(patternIndex));
      case TIMESTAMP:
        return new TimeStampDefn(name, patternIndex, formatConfig.getDateFormat(patternIndex));
      case TIME:
        return new TimeDefn(name, patternIndex, formatConfig.getDateFormat(patternIndex));
      default:
        throw UserException
            .validationError()
            .message("Undefined column types")
            .addContext("Position", patternIndex)
            .addContext("Field name", name)
            .addContext("Type", typeName)
            .build(logger);
    }
  }

  private void defineVectors() {
    for (int i = 0; i < columns.length; i++) {
      try {
        columns[i].define(outputMutator);
      } catch (SchemaChangeException e) {
        throw UserException
            .systemError(e)
            .message("Vector creation failed")
            .build(logger);
      }
    }
  }

  @Override
  public int next() {
    rowIndex = 0;
    while (nextLine()) {
    }
    return rowIndex;
  }

  private boolean nextLine() {
    String line;
    try {
      line = reader.readLine();
    } catch (IOException e) {
      throw UserException
          .dataReadError(e)
          .message("Error reading file:")
          .addContext("File", fileWork.getPath())
          .build(logger);
    }

    if (line == null) {
      return false;
    }
    Matcher lineMatcher = pattern.matcher(line);
    if (lineMatcher.matches()) {
      loadVectors(lineMatcher);
      return rowIndex < BATCH_SIZE;
    }

    errorCount++;
    if (errorCount < maxErrors) {
      logger.warn("Unmatached line: {}", line);
    } else if (errorCount > maxErrors) {
      throw UserException.parseError()
          .message("Too many errors.  Max error threshold exceeded.")
          .addContext("Line", line)
          .addContext("Line number", rowIndex)
          .build(logger);
    }
    //If the user asked for the unmatched columns display them
    if (unmatchedRows) {
      //If the user asked for the unmatched columns AND other columns
      if (columns.length > 1) {
        columns[unmatchedColumnIndex].load(rowIndex, line);
        rowIndex++;
        return rowIndex < BATCH_SIZE;
      } else {
        //If the user ONLY asked for the unmatched columns
        columns[unmatchedColumnIndex].load(unmatchedRowIndex, line);
        unmatchedRowIndex++;
        rowIndex = unmatchedRowIndex;
        return unmatchedRowIndex < BATCH_SIZE;
      }
    }

    return true;
  }

  private void loadVectors(Matcher m) {
    String value = null;
    /*if( unmatchedRows && columns.length == 1 ){
      return;
    }*/

    for (int i = 0; i < columns.length; i++) {
      //Skip the unmatched rows column
      if (columns[i].name.equals("_unmatched_rows")) {
        continue;
      }

      if (columns[i].index >= 0) {
        //Get the value of the regex group
        value = m.group(columns[i].index + 1);

        //If the value is not null, assign it to the column
        if (value != null) {
          columns[i].load(rowIndex, value);
        }
      } else if (columns[i].name.equals("_raw")) {
        //Special case.  The first is if the query contains the _raw column
        value = m.group(0);
        if (value != null) {
          columns[i].load(rowIndex, value);
        } else {
          rowIndex++;
        }
      }
    }
    rowIndex++;
  }

  @Override
  public void close() {
    if (reader != null) {
      try {
        reader.close();
      } catch (IOException e) {
        logger.warn("Error when closing file: " + fileWork.getPath(), e);
      }
      reader = null;
    }
  }

  @Override
  public String toString() {
    return "LogRecordReader[File=" + fileWork.getPath()
        + ", Line=" + rowIndex
        + "]";
  }
}
