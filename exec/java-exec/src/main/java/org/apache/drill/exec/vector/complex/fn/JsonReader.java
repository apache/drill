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
package org.apache.drill.exec.vector.complex.fn;

import io.netty.buffer.DrillBuf;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.expression.PathSegment;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.store.easy.json.JsonProcessor;
import org.apache.drill.exec.store.easy.json.reader.BaseJsonProcessor;
import org.apache.drill.exec.vector.complex.fn.VectorOutput.ListVectorOutput;
import org.apache.drill.exec.vector.complex.fn.VectorOutput.MapVectorOutput;
import org.apache.drill.exec.vector.complex.writer.BaseWriter;
import org.apache.drill.exec.vector.complex.writer.BaseWriter.ComplexWriter;
import org.apache.drill.exec.vector.complex.writer.BaseWriter.ListWriter;
import org.apache.drill.exec.vector.complex.writer.BaseWriter.MapWriter;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;

public class JsonReader extends BaseJsonProcessor {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(JsonReader.class);
  public final static int MAX_RECORD_SIZE = 128 * 1024;

  private final WorkingBuffer workingBuffer;
  private final List<SchemaPath> columns;
  private final boolean allTextMode;
  private boolean atLeastOneWrite = false;
  private final MapVectorOutput mapOutput;
  private final ListVectorOutput listOutput;
  private final boolean extended = true;
  private final boolean readNumbersAsDouble;

  /**
   * Describes whether or not this reader can unwrap a single root array record and treat it like a set of distinct records.
   */
  private final boolean skipOuterList;

  /**
   * Whether the reader is currently in a situation where we are unwrapping an outer list.
   */
  private boolean inOuterList;
  /**
   * The name of the current field being parsed. For Error messages.
   */
  private String currentFieldName;

  private FieldSelection selection;

  public JsonReader(DrillBuf managedBuf, boolean allTextMode, boolean skipOuterList, boolean readNumbersAsDouble) {
    this(managedBuf, GroupScan.ALL_COLUMNS, allTextMode, skipOuterList, readNumbersAsDouble);
  }

  public JsonReader(DrillBuf managedBuf, List<SchemaPath> columns, boolean allTextMode, boolean skipOuterList, boolean readNumbersAsDouble) {
    super(managedBuf);
    assert Preconditions.checkNotNull(columns).size() > 0 : "json record reader requires at least a column";
    this.selection = FieldSelection.getFieldSelection(columns);
    this.workingBuffer = new WorkingBuffer(managedBuf);
    this.skipOuterList = skipOuterList;
    this.allTextMode = allTextMode;
    this.columns = columns;
    this.mapOutput = new MapVectorOutput(workingBuffer);
    this.listOutput = new ListVectorOutput(workingBuffer);
    this.currentFieldName="<none>";
    this.readNumbersAsDouble = readNumbersAsDouble;
  }

  @Override
  public void ensureAtLeastOneField(ComplexWriter writer) {
    if (!atLeastOneWrite) {
      // if we had no columns, create one empty one so we can return some data for count purposes.
      SchemaPath sp = columns.get(0);
      PathSegment root = sp.getRootSegment();
      BaseWriter.MapWriter fieldWriter = writer.rootAsMap();
      while (root.getChild() != null && !root.getChild().isArray()) {
        fieldWriter = fieldWriter.map(root.getNameSegment().getPath());
        root = root.getChild();
      }
      fieldWriter.integer(root.getNameSegment().getPath());
    }
  }

  public void setSource(int start, int end, DrillBuf buf) throws IOException {
    setSource(DrillBufInputStream.getStream(start, end, buf));
  }


  @Override
  public void setSource(InputStream is) throws IOException {
    super.setSource(is);
    mapOutput.setParser(parser);
    listOutput.setParser(parser);
  }

  @Override
  public void setSource(JsonNode node) {
    super.setSource(node);
    mapOutput.setParser(parser);
    listOutput.setParser(parser);
  }

  public void setSource(String data) throws IOException {
    setSource(data.getBytes(Charsets.UTF_8));
  }

  public void setSource(byte[] bytes) throws IOException {
    setSource(new SeekableBAIS(bytes));
  }

  @Override
  public ReadState write(ComplexWriter writer) throws IOException {
    JsonToken t = parser.nextToken();

    while (!parser.hasCurrentToken() && !parser.isClosed()) {
      t = parser.nextToken();
    }

    if (parser.isClosed()) {
      return ReadState.END_OF_STREAM;
    }

    ReadState readState = writeToVector(writer, t);

    switch (readState) {
    case END_OF_STREAM:
      break;
    case WRITE_SUCCEED:
      break;
    default:
      throw
        getExceptionWithContext(
          UserException.dataReadError(), currentFieldName, null)
          .message("Failure while reading JSON. (Got an invalid read state %s )", readState.toString())
          .build(logger);
    }

    return readState;
  }

  private void confirmLast() throws IOException{
    parser.nextToken();
    if(!parser.isClosed()){
      throw
        getExceptionWithContext(
          UserException.dataReadError(), currentFieldName, null)
        .message("Drill attempted to unwrap a toplevel list "
          + "in your document.  However, it appears that there is trailing content after this top level list.  Drill only "
          + "supports querying a set of distinct maps or a single json array with multiple inner maps.")
        .build(logger);
    }
  }

  private ReadState writeToVector(ComplexWriter writer, JsonToken t) throws IOException {
    switch (t) {
    case START_OBJECT:
      writeDataSwitch(writer.rootAsMap());
      break;
    case START_ARRAY:
      if(inOuterList){
        throw
          getExceptionWithContext(
            UserException.dataReadError(), currentFieldName, null)
          .message("The top level of your document must either be a single array of maps or a set "
            + "of white space delimited maps.")
          .build(logger);
      }

      if(skipOuterList){
        t = parser.nextToken();
        if(t == JsonToken.START_OBJECT){
          inOuterList = true;
          writeDataSwitch(writer.rootAsMap());
        }else{
          throw
            getExceptionWithContext(
              UserException.dataReadError(), currentFieldName, null)
            .message("The top level of your document must either be a single array of maps or a set "
              + "of white space delimited maps.")
            .build(logger);
        }

      }else{
        writeDataSwitch(writer.rootAsList());
      }
      break;
    case END_ARRAY:

      if(inOuterList){
        confirmLast();
        return ReadState.END_OF_STREAM;
      }else{
        throw
          getExceptionWithContext(
            UserException.dataReadError(), currentFieldName, null)
          .message("Failure while parsing JSON.  Ran across unexpected %s.", JsonToken.END_ARRAY)
          .build(logger);
      }

    case NOT_AVAILABLE:
      return ReadState.END_OF_STREAM;
    default:
      throw
        getExceptionWithContext(
          UserException.dataReadError(), currentFieldName, null)
          .message("Failure while parsing JSON.  Found token of [%s].  Drill currently only supports parsing "
              + "json strings that contain either lists or maps.  The root object cannot be a scalar.", t)
          .build(logger);
    }

    return ReadState.WRITE_SUCCEED;

  }

  private void writeDataSwitch(MapWriter w) throws IOException {
    if (this.allTextMode) {
      writeDataAllText(w, this.selection, true);
    } else {
      writeData(w, this.selection, true);
    }
  }

  private void writeDataSwitch(ListWriter w) throws IOException {
    if (this.allTextMode) {
      writeDataAllText(w);
    } else {
      writeData(w);
    }
  }

  private void consumeEntireNextValue() throws IOException {
    switch (parser.nextToken()) {
    case START_ARRAY:
    case START_OBJECT:
      parser.skipChildren();
      return;
    default:
      // hit a single value, do nothing as the token was already read
      // in the switch statement
      return;
    }
  }

  /**
   *
   * @param map
   * @param selection
   * @param moveForward
   *          Whether or not we should start with using the current token or the next token. If moveForward = true, we
   *          should start with the next token and ignore the current one.
   * @throws IOException
   */
  private void writeData(MapWriter map, FieldSelection selection, boolean moveForward) throws IOException {
    //
    map.start();
    outside: while (true) {

      JsonToken t;
      if(moveForward){
        t = parser.nextToken();
      }else{
        t = parser.getCurrentToken();
        moveForward = true;
      }

      if (t == JsonToken.NOT_AVAILABLE || t == JsonToken.END_OBJECT) {
        return;
      }

      assert t == JsonToken.FIELD_NAME : String.format("Expected FIELD_NAME but got %s.", t.name());

      final String fieldName = parser.getText();
      this.currentFieldName = fieldName;
      FieldSelection childSelection = selection.getChild(fieldName);
      if (childSelection.isNeverValid()) {
        consumeEntireNextValue();
        continue outside;
      }

      switch (parser.nextToken()) {
      case START_ARRAY:
        writeData(map.list(fieldName));
        break;
      case START_OBJECT:
        if (!writeMapDataIfTyped(map, fieldName)) {
          writeData(map.map(fieldName), childSelection, false);
        }
        break;
      case END_OBJECT:
        break outside;

      case VALUE_FALSE: {
        map.bit(fieldName).writeBit(0);
        atLeastOneWrite = true;
        break;
      }
      case VALUE_TRUE: {
        map.bit(fieldName).writeBit(1);
        atLeastOneWrite = true;
        break;
      }
      case VALUE_NULL:
        // do nothing as we don't have a type.
        break;
      case VALUE_NUMBER_FLOAT:
        map.float8(fieldName).writeFloat8(parser.getDoubleValue());
        atLeastOneWrite = true;
        break;
      case VALUE_NUMBER_INT:
        if (this.readNumbersAsDouble) {
          map.float8(fieldName).writeFloat8(parser.getDoubleValue());
        }
        else {
          map.bigInt(fieldName).writeBigInt(parser.getLongValue());
        }
        atLeastOneWrite = true;
        break;
      case VALUE_STRING:
        handleString(parser, map, fieldName);
        atLeastOneWrite = true;
        break;

      default:
        throw
          getExceptionWithContext(
            UserException.dataReadError(), currentFieldName, null)
          .message("Unexpected token %s", parser.getCurrentToken())
          .build(logger);
      }

    }
    map.end();

  }

  private void writeDataAllText(MapWriter map, FieldSelection selection, boolean moveForward) throws IOException {
    //
    map.start();
    outside: while (true) {


      JsonToken t;

      if(moveForward){
        t = parser.nextToken();
      }else{
        t = parser.getCurrentToken();
        moveForward = true;
      }

      if (t == JsonToken.NOT_AVAILABLE || t == JsonToken.END_OBJECT) {
        return;
      }

      assert t == JsonToken.FIELD_NAME : String.format("Expected FIELD_NAME but got %s.", t.name());

      final String fieldName = parser.getText();
      this.currentFieldName = fieldName;
      FieldSelection childSelection = selection.getChild(fieldName);
      if (childSelection.isNeverValid()) {
        consumeEntireNextValue();
        continue outside;
      }

      switch (parser.nextToken()) {
      case START_ARRAY:
        writeDataAllText(map.list(fieldName));
        break;
      case START_OBJECT:
        if (!writeMapDataIfTyped(map, fieldName)) {
          writeDataAllText(map.map(fieldName), childSelection, false);
        }
        break;
      case END_OBJECT:
        break outside;

      case VALUE_EMBEDDED_OBJECT:
      case VALUE_FALSE:
      case VALUE_TRUE:
      case VALUE_NUMBER_FLOAT:
      case VALUE_NUMBER_INT:
      case VALUE_STRING:
        handleString(parser, map, fieldName);
        atLeastOneWrite = true;
        break;
      case VALUE_NULL:
        // do nothing as we don't have a type.
        break;

      default:
        throw
          getExceptionWithContext(
            UserException.dataReadError(), currentFieldName, null)
          .message("Unexpected token %s", parser.getCurrentToken())
          .build(logger);
      }
    }
    map.end();

  }

  /**
   * Will attempt to take the current value and consume it as an extended value (if extended mode is enabled).  Whether extended is enable or disabled, will consume the next token in the stream.
   * @param writer
   * @param fieldName
   * @return
   * @throws IOException
   */
  private boolean writeMapDataIfTyped(MapWriter writer, String fieldName) throws IOException {
    if (extended) {
      atLeastOneWrite = true;
      return mapOutput.run(writer, fieldName);
    } else {
      parser.nextToken();
      return false;
    }
  }

  /**
   * Will attempt to take the current value and consume it as an extended value (if extended mode is enabled).  Whether extended is enable or disabled, will consume the next token in the stream.
   * @param writer
   * @return
   * @throws IOException
   */
  private boolean writeListDataIfTyped(ListWriter writer) throws IOException {
    if (extended) {
      atLeastOneWrite = true;
      return listOutput.run(writer);
    } else {
      parser.nextToken();
      return false;
    }
  }

  private void handleString(JsonParser parser, MapWriter writer, String fieldName) throws IOException {
    writer.varChar(fieldName).writeVarChar(0, workingBuffer.prepareVarCharHolder(parser.getText()),
        workingBuffer.getBuf());
  }

  private void handleString(JsonParser parser, ListWriter writer) throws IOException {
    writer.varChar().writeVarChar(0, workingBuffer.prepareVarCharHolder(parser.getText()), workingBuffer.getBuf());
  }

  private void writeData(ListWriter list) throws IOException {
    list.start();
    outside: while (true) {
      try {
      switch (parser.nextToken()) {
      case START_ARRAY:
        writeData(list.list());
        break;
      case START_OBJECT:
        if (!writeListDataIfTyped(list)) {
          writeData(list.map(), FieldSelection.ALL_VALID, false);
        }
        break;
      case END_ARRAY:
      case END_OBJECT:
        break outside;

      case VALUE_EMBEDDED_OBJECT:
      case VALUE_FALSE: {
        list.bit().writeBit(0);
        atLeastOneWrite = true;
        break;
      }
      case VALUE_TRUE: {
        list.bit().writeBit(1);
        atLeastOneWrite = true;
        break;
      }
      case VALUE_NULL:
        throw UserException.unsupportedError()
          .message("Null values are not supported in lists by default. " +
            "Please set `store.json.all_text_mode` to true to read lists containing nulls. " +
            "Be advised that this will treat JSON null values as a string containing the word 'null'.")
          .build(logger);
      case VALUE_NUMBER_FLOAT:
        list.float8().writeFloat8(parser.getDoubleValue());
        atLeastOneWrite = true;
        break;
      case VALUE_NUMBER_INT:
        if (this.readNumbersAsDouble) {
          list.float8().writeFloat8(parser.getDoubleValue());
        }
        else {
          list.bigInt().writeBigInt(parser.getLongValue());
        }
        atLeastOneWrite = true;
        break;
      case VALUE_STRING:
        handleString(parser, list);
        atLeastOneWrite = true;
        break;
      default:
        throw UserException.dataReadError()
          .message("Unexpected token %s", parser.getCurrentToken())
          .build(logger);
    }
    } catch (Exception e) {
      throw getExceptionWithContext(e, this.currentFieldName, null).build(logger);
    }
    }
    list.end();

  }

  private void writeDataAllText(ListWriter list) throws IOException {
    list.start();
    outside: while (true) {

      switch (parser.nextToken()) {
      case START_ARRAY:
        writeDataAllText(list.list());
        break;
      case START_OBJECT:
        if (!writeListDataIfTyped(list)) {
          writeDataAllText(list.map(), FieldSelection.ALL_VALID, false);
        }
        break;
      case END_ARRAY:
      case END_OBJECT:
        break outside;

      case VALUE_EMBEDDED_OBJECT:
      case VALUE_FALSE:
      case VALUE_TRUE:
      case VALUE_NULL:
      case VALUE_NUMBER_FLOAT:
      case VALUE_NUMBER_INT:
      case VALUE_STRING:
        handleString(parser, list);
        atLeastOneWrite = true;
        break;
      default:
        throw
          getExceptionWithContext(
            UserException.dataReadError(), currentFieldName, null)
          .message("Unexpected token %s", parser.getCurrentToken())
          .build(logger);
      }
    }
    list.end();

  }

  public DrillBuf getWorkBuf() {
    return workingBuffer.getBuf();
  }

}
