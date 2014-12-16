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
import java.util.ArrayList;
import java.util.List;

import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.common.expression.PathSegment;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.expr.holders.BigIntHolder;
import org.apache.drill.exec.expr.holders.BitHolder;
import org.apache.drill.exec.expr.holders.Float8Holder;
import org.apache.drill.exec.expr.holders.VarCharHolder;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.store.AbstractRecordReader;
import org.apache.drill.exec.store.easy.json.RewindableUtf8Reader;
import org.apache.drill.exec.vector.complex.writer.BaseWriter;
import org.apache.drill.exec.vector.complex.writer.BaseWriter.ComplexWriter;
import org.apache.drill.exec.vector.complex.writer.BaseWriter.ListWriter;
import org.apache.drill.exec.vector.complex.writer.BaseWriter.MapWriter;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Seekable;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonParser.Feature;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.io.IOContext;
import com.fasterxml.jackson.core.sym.BytesToNameCanonicalizer;
import com.fasterxml.jackson.core.util.BufferRecycler;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import org.apache.hadoop.io.compress.CompressionInputStream;

public class JsonReader {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(JsonReader.class);
  public final static int MAX_RECORD_SIZE = 128*1024;

  private final RewindableUtf8Reader parser;
  private DrillBuf workBuf;
  private final List<SchemaPath> columns;
  private final boolean allTextMode;
  private boolean atLeastOneWrite = false;

  private FieldSelection selection;

  /**
   * Whether we are in a reset state. In a reset state, we don't have to advance to the next token on write because
   * we're already at the start of the next object
   */
  private boolean onReset = false;

  public static enum ReadState {
    WRITE_FAILURE,
    END_OF_STREAM,
    WRITE_SUCCEED
  }

  public JsonReader() throws IOException {
    this(null, false);
  }

  public JsonReader(DrillBuf managedBuf, boolean allTextMode) {
    this(managedBuf, GroupScan.ALL_COLUMNS, allTextMode);
  }

  public JsonReader(DrillBuf managedBuf, List<SchemaPath> columns, boolean allTextMode) {
    BufferRecycler recycler = new BufferRecycler();
    IOContext context = new IOContext(recycler, this, false);
    final int features = JsonParser.Feature.collectDefaults() //
        | Feature.ALLOW_COMMENTS.getMask() //
        | Feature.ALLOW_UNQUOTED_FIELD_NAMES.getMask();

    BytesToNameCanonicalizer can = BytesToNameCanonicalizer.createRoot();
    parser = new RewindableUtf8Reader<>(context, features, can.makeChild(JsonFactory.Feature.collectDefaults()), context.allocReadIOBuffer());

    assert Preconditions.checkNotNull(columns).size() > 0 : "json record reader requires at least a column";

    this.selection = FieldSelection.getFieldSelection(columns);
    this.workBuf = managedBuf;
    this.allTextMode = allTextMode;
    this.columns = columns;
  }

  public void ensureAtLeastOneField(ComplexWriter writer){
    if(!atLeastOneWrite){
      // if we had no columns, create one empty one so we can return some data for count purposes.
      SchemaPath sp = columns.get(0);
      PathSegment root = sp.getRootSegment();
      BaseWriter.MapWriter fieldWriter = writer.rootAsMap();
      while (root.getChild() != null && ! root.getChild().isArray()) {
        fieldWriter = fieldWriter.map(root.getNameSegment().getPath());
        root = root.getChild();
      }
      fieldWriter.integer(root.getNameSegment().getPath());
    }
  }

  public void setSource(InputStream is) throws IOException{
    parser.setInputStream(is);
    this.onReset = false;
  }

  public void setSource(int start, int end, DrillBuf buf) throws IOException{
    parser.setInputStream(DrillBufInputStream.getStream(start, end, buf));
  }

  public void setSource(String data) throws IOException {
    setSource(data.getBytes(Charsets.UTF_8));
  }

  public void setSource(byte[] bytes) throws IOException{
    parser.setInputStream(new SeekableBAIS(bytes));
    this.onReset = false;
  }


  public ReadState write(ComplexWriter writer) throws IOException {
    JsonToken t = onReset ? parser.getCurrentToken() : parser.nextToken();

    while (!parser.hasCurrentToken() && parser.hasDataAvailable()) {
      t = parser.nextToken();
    }

    if(!parser.hasCurrentToken()){
      return ReadState.END_OF_STREAM;
    }

    if(onReset){
      onReset = false;
    }else{
      parser.mark();
    }

    ReadState readState = writeToVector(writer, t);

    switch(readState){
    case END_OF_STREAM:
      break;
    case WRITE_FAILURE:
      logger.debug("Ran out of space while writing object, rewinding to object start.");
      parser.resetToMark();
      onReset = true;
      break;
    case WRITE_SUCCEED:
      break;
    default:
      throw new IllegalStateException();
    }

    return readState;
  }

  private ReadState writeToVector(ComplexWriter writer, JsonToken t) throws IOException {
    if (!writer.ok()) {
      return ReadState.WRITE_FAILURE;
    }

    switch (t) {
      case START_OBJECT:
        writeDataSwitch(writer.rootAsMap());
        break;
      case START_ARRAY:
        writeDataSwitch(writer.rootAsList());
        break;
      case NOT_AVAILABLE:
        return ReadState.END_OF_STREAM;
      default:
        throw new JsonParseException(
            String.format("Failure while parsing JSON.  Found token of [%s]  Drill currently only supports parsing "
                + "json strings that contain either lists or maps.  The root object cannot be a scalar.",
                t),
            parser.getCurrentLocation());
      }

      if(writer.ok()){
        return ReadState.WRITE_SUCCEED;
      }else{
        return ReadState.WRITE_FAILURE;
      }
  }

  private void writeDataSwitch(MapWriter w) throws IOException{
    if(this.allTextMode){
      writeDataAllText(w, this.selection);
    }else{
      writeData(w, this.selection);
    }
  }

  private void writeDataSwitch(ListWriter w) throws IOException{
    if(this.allTextMode){
      writeDataAllText(w);
    }else{
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

  private void writeData(MapWriter map, FieldSelection selection) throws IOException {
    //
    map.start();
    outside: while(true) {
      if (!map.ok()) {
        return;
      }
      JsonToken t = parser.nextToken();
      if (t == JsonToken.NOT_AVAILABLE || t == JsonToken.END_OBJECT) {
        return;
      }

      assert t == JsonToken.FIELD_NAME : String.format("Expected FIELD_NAME but got %s.", t.name());

      final String fieldName = parser.getText();
      FieldSelection childSelection = selection.getChild(fieldName);
      if(childSelection.isNeverValid()){
        consumeEntireNextValue();
        continue outside;
      }

      switch(parser.nextToken()) {
      case START_ARRAY:
        writeData(map.list(fieldName));
        break;
      case START_OBJECT:
        writeData(map.map(fieldName), childSelection);
        break;
      case END_OBJECT:
        break outside;

      case VALUE_EMBEDDED_OBJECT:
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
        // do check value capacity only if vector is allocated.
        if (map.getValueCapacity() > 0) {
          map.checkValueCapacity();
        }
        // do nothing as we don't have a type.
        break;
      case VALUE_NUMBER_FLOAT:
        map.float8(fieldName).writeFloat8(parser.getDoubleValue());
        atLeastOneWrite = true;
        break;
      case VALUE_NUMBER_INT:
        map.bigInt(fieldName).writeBigInt(parser.getLongValue());
        atLeastOneWrite = true;
        break;
      case VALUE_STRING:
        handleString(parser, map, fieldName);
        atLeastOneWrite = true;
        break;

      default:
        throw new IllegalStateException("Unexpected token " + parser.getCurrentToken());

      }
    }
    map.end();

  }


  private void writeDataAllText(MapWriter map, FieldSelection selection) throws IOException {
    //
    map.start();
    outside: while(true) {
      if (!map.ok()) {
        return;
      }
      JsonToken t = parser.nextToken();
      if (t == JsonToken.NOT_AVAILABLE || t == JsonToken.END_OBJECT) {
        return;
      }

      assert t == JsonToken.FIELD_NAME : String.format("Expected FIELD_NAME but got %s.", t.name());

      final String fieldName = parser.getText();
      FieldSelection childSelection = selection.getChild(fieldName);
      if(childSelection.isNeverValid()){
        consumeEntireNextValue();
        continue outside;
      }


      switch(parser.nextToken()) {
      case START_ARRAY:
        writeDataAllText(map.list(fieldName));
        break;
      case START_OBJECT:
        writeDataAllText(map.map(fieldName), childSelection);
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
        // do check value capacity only if vector is allocated.
        if (map.getValueCapacity() > 0) {
          map.checkValueCapacity();
        }
        // do nothing as we don't have a type.
        break;


      default:
        throw new IllegalStateException("Unexpected token " + parser.getCurrentToken());

      }
    }
    map.end();

  }


  private void ensure(int length) {
    workBuf = workBuf.reallocIfNeeded(length);
  }

  private int prepareVarCharHolder(String value) throws IOException {
    byte[] b = value.getBytes(Charsets.UTF_8);
    ensure(b.length);
    workBuf.setBytes(0, b);
    return b.length;
  }

  private void handleString(JsonParser parser, MapWriter writer, String fieldName) throws IOException {
    writer.varChar(fieldName).writeVarChar(0, prepareVarCharHolder(parser.getText()), workBuf);
  }

  private void handleString(JsonParser parser, ListWriter writer) throws IOException {
    writer.varChar().writeVarChar(0, prepareVarCharHolder(parser.getText()), workBuf);
  }

  private void writeData(ListWriter list) throws IOException {
    list.start();
    outside: while (true) {
      if (!list.ok()) {
        return;
      }

      switch (parser.nextToken()) {
      case START_ARRAY:
        writeData(list.list());
        break;
      case START_OBJECT:
        writeData(list.map(), FieldSelection.ALL_VALID);
        break;
      case END_ARRAY:
      case END_OBJECT:
        break outside;

      case VALUE_EMBEDDED_OBJECT:
      case VALUE_FALSE:{
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
        throw new DrillRuntimeException("Null values are not supported in lists be default. " +
            "Please set `store.json.all_text_mode` to true to read lists containing nulls. " +
            "Be advised that this will treat JSON null values as string containing the word 'null'.");
      case VALUE_NUMBER_FLOAT:
        list.float8().writeFloat8(parser.getDoubleValue());
        atLeastOneWrite = true;
        break;
      case VALUE_NUMBER_INT:
        list.bigInt().writeBigInt(parser.getLongValue());
        atLeastOneWrite = true;
        break;
      case VALUE_STRING:
        handleString(parser, list);
        atLeastOneWrite = true;
        break;
      default:
        throw new IllegalStateException("Unexpected token " + parser.getCurrentToken());
      }
    }
    list.end();

  }

  private void writeDataAllText(ListWriter list) throws IOException {
    list.start();
    outside: while (true) {
      if (!list.ok()) {
        return;
      }

      switch (parser.nextToken()) {
      case START_ARRAY:
        writeDataAllText(list.list());
        break;
      case START_OBJECT:
        writeDataAllText(list.map(), FieldSelection.ALL_VALID);
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
        throw new IllegalStateException("Unexpected token " + parser.getCurrentToken());
      }
    }
    list.end();

  }

  public DrillBuf getWorkBuf() {
    return workBuf;
  }

}
