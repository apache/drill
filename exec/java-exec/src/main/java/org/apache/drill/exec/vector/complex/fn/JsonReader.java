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
import com.google.common.base.Preconditions;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.UnpooledByteBufAllocator;

import java.io.IOException;
import java.io.Reader;
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
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.store.AbstractRecordReader;
import org.apache.drill.exec.vector.complex.writer.BaseWriter;
import org.apache.drill.exec.vector.complex.writer.BaseWriter.ComplexWriter;
import org.apache.drill.exec.vector.complex.writer.BaseWriter.ListWriter;
import org.apache.drill.exec.vector.complex.writer.BaseWriter.MapWriter;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonParser.Feature;
import com.fasterxml.jackson.core.JsonToken;
import com.google.common.base.Charsets;

public class JsonReader {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(JsonReader.class);
  public final static int MAX_RECORD_SIZE = 128*1024;



  private final JsonFactory factory = new JsonFactory();
  private JsonParser parser;
  private DrillBuf workBuf;
  private List<SchemaPath> columns;
  // This is a parallel array for the field above to indicate if we have found any values in a
  // given selected column. This allows for columns that are requested to receive a vector full of
  // null values if no values were found in an entire read. The reason this needs to happen after
  // all of the records have been read in a batch is to prevent a schema change when we actually find
  // data in that column.
  private boolean[] columnsFound;
  // A flag set at setup time if the start column is in the requested column list, prevents
  // doing a more computational intensive check if we are supposed to be reading a column
  private boolean starRequested;
  private boolean allTextMode;

  public JsonReader() throws IOException {
    this(null, false);
  }

  public JsonReader(DrillBuf managedBuf, boolean allTextMode) throws IOException {
    this(managedBuf, GroupScan.ALL_COLUMNS, allTextMode);
  }

  public JsonReader(DrillBuf managedBuf, List<SchemaPath> columns, boolean allTextMode) throws JsonParseException, IOException {
    factory.configure(Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);
    factory.configure(Feature.ALLOW_COMMENTS, true);
    assert Preconditions.checkNotNull(columns).size() > 0 : "json record reader requires at least a column";
    this.columns = columns;
    this.starRequested = containsStar();
    this.workBuf = managedBuf;
    this.allTextMode = allTextMode;
    this.columnsFound = new boolean[this.columns.size()];
  }

  private boolean containsStar() {
    for (SchemaPath expr : this.columns){
      if (expr.getRootSegment().getPath().equals("*"))
        return true;
    }
    return false;
  }

  private boolean fieldSelected(SchemaPath field){
    if (starRequested)
      return true;
    int i = 0;
    for (SchemaPath expr : this.columns){
      if ( expr.contains(field)){
        columnsFound[i] = true;
        return true;
      }
      i++;
    }
    return false;
  }

  public List<SchemaPath> getNullColumns() {
    ArrayList<SchemaPath> nullColumns = new ArrayList<SchemaPath>();
    for (int i = 0; i < columnsFound.length; i++ ) {
      if ( ! columnsFound[i] && !columns.get(i).equals(AbstractRecordReader.STAR_COLUMN)) {
        nullColumns.add(columns.get(i));
      }
    }
    return nullColumns;
  }

  public boolean write(Reader reader, ComplexWriter writer) throws JsonParseException, IOException {

    parser = factory.createJsonParser(reader);
    reader.mark(MAX_RECORD_SIZE);
    JsonToken t = parser.nextToken();
    while(!parser.hasCurrentToken()) t = parser.nextToken();


    switch (t) {
    case START_OBJECT:
      writeData(writer.rootAsMap());
      break;
    case START_ARRAY:
      writeData(writer.rootAsList());
      break;
    case NOT_AVAILABLE:
      return false;
    default:
      throw new JsonParseException(
          String.format("Failure while parsing JSON.  Found token of [%s]  Drill currently only supports parsing "
              + "json strings that contain either lists or maps.  The root object cannot be a scalar.",
              t),
          parser.getCurrentLocation());
    }

    return true;
  }

  private void consumeEntireNextValue(JsonParser parser) throws IOException {
    switch(parser.nextToken()){
      case START_ARRAY:
      case START_OBJECT:
        int arrayAndObjectCounter = 1;
        skipArrayLoop: while (true) {
          switch(parser.nextToken()) {
            case START_ARRAY:
            case START_OBJECT:
              arrayAndObjectCounter++;
              break;
            case END_ARRAY:
            case END_OBJECT:
              arrayAndObjectCounter--;
              if (arrayAndObjectCounter == 0) {
                break skipArrayLoop;
              }
              break;
          }
        }
        break;
      default:
        // hit a single value, do nothing as the token was already read
        // in the switch statement
        break;
    }
  }

  private void writeData(MapWriter map) throws JsonParseException, IOException {
    //
    map.start();
    outside: while(true){
      JsonToken t = parser.nextToken();
      if(t == JsonToken.NOT_AVAILABLE || t == JsonToken.END_OBJECT) return;

      assert t == JsonToken.FIELD_NAME : String.format("Expected FIELD_NAME but got %s.", t.name());
      final String fieldName = parser.getText();
      SchemaPath path;
      if (map.getField().getPath().getRootSegment().getPath().equals("")) {
        path = new SchemaPath(new PathSegment.NameSegment(fieldName));
      } else {
        path = map.getField().getPath().getChild(fieldName);
      }
      if ( ! fieldSelected(path) ) {
        consumeEntireNextValue(parser);
        continue outside;
      }

      switch(parser.nextToken()){
      case START_ARRAY:
        writeData(map.list(fieldName));
        break;
      case START_OBJECT:
        writeData(map.map(fieldName));
        break;
      case END_OBJECT:
        break outside;

      case VALUE_EMBEDDED_OBJECT:
      case VALUE_FALSE: {
        if (allTextMode) {
          handleString(parser, map, fieldName);
          break;
        }
        BitHolder h = new BitHolder();
        h.value = 0;
        map.bit(fieldName).write(h);
        break;
      }
      case VALUE_TRUE: {
        if (allTextMode) {
          handleString(parser, map, fieldName);
          break;
        }
        BitHolder h = new BitHolder();
        h.value = 1;
        map.bit(fieldName).write(h);
        break;
      }
      case VALUE_NULL:
        if (allTextMode) {
          map.checkValueCapacity();
          break;
        }
        map.checkValueCapacity();
        // do nothing as we don't have a type.
        break;
      case VALUE_NUMBER_FLOAT:
        if (allTextMode) {
          handleString(parser, map, fieldName);
          break;
        }
        Float8Holder fh = new Float8Holder();
        fh.value = parser.getDoubleValue();
        map.float8(fieldName).write(fh);
        break;
      case VALUE_NUMBER_INT:
        if (allTextMode) {
          handleString(parser, map, fieldName);
          break;
        }
        BigIntHolder bh = new BigIntHolder();
        bh.value = parser.getLongValue();
        map.bigInt(fieldName).write(bh);
        break;
      case VALUE_STRING:
        handleString(parser, map, fieldName);
        break;

      default:
        throw new IllegalStateException("Unexpected token " + parser.getCurrentToken());

      }

    }
    map.end();

  }

  private void ensure(int length){
    workBuf = workBuf.reallocIfNeeded(length);
  }

  private VarCharHolder prepareVarCharHolder(VarCharHolder vh, String value) throws IOException {
    byte[] b = value.getBytes(Charsets.UTF_8);
    ensure(b.length);
    workBuf.setBytes(0, b);
    vh.buffer = workBuf;
    vh.start = 0;
    vh.end = b.length;
    return vh;
  }

  private void handleString(JsonParser parser, MapWriter writer, String fieldName) throws IOException {
    VarCharHolder vh = new VarCharHolder();
    writer.varChar(fieldName).write(prepareVarCharHolder(vh, parser.getText()));
  }

  private void handleString(JsonParser parser, ListWriter writer) throws IOException {
    VarCharHolder vh = new VarCharHolder();
    writer.varChar().write(prepareVarCharHolder(vh, parser.getText()));
  }

  private void handleString(String value, ListWriter writer) throws IOException {
    VarCharHolder vh = new VarCharHolder();
    writer.varChar().write(prepareVarCharHolder(vh, parser.getText()));
  }

  private void writeData(ListWriter list) throws JsonParseException, IOException {
    list.start();
    outside: while(true){

      switch(parser.nextToken()){
      case START_ARRAY:
        writeData(list.list());
        break;
      case START_OBJECT:
        writeData(list.map());
        break;
      case END_ARRAY:
      case END_OBJECT:
        break outside;

      case VALUE_EMBEDDED_OBJECT:
      case VALUE_FALSE:{
        if (allTextMode) {
          handleString(parser, list);
          break;
        }
        BitHolder h = new BitHolder();
        h.value = 0;
        list.bit().write(h);
        break;
      }
      case VALUE_TRUE: {
        if (allTextMode) {
          handleString(parser, list);
          break;
        }
        BitHolder h = new BitHolder();
        h.value = 1;
        list.bit().write(h);
        break;
      }
      case VALUE_NULL:
        if (allTextMode) {
          handleString("null", list);
          break;
        }
        throw new DrillRuntimeException("Null values are not supported in lists be default. " +
            "Please set jason_all_text_mode to true to read lists containing nulls. " +
            "Be advised that this will treat JSON null values as string containing the word 'null'.");
      case VALUE_NUMBER_FLOAT:
        if (allTextMode) {
          handleString(parser, list);
          break;
        }
        Float8Holder fh = new Float8Holder();
        fh.value = parser.getDoubleValue();
        list.float8().write(fh);
        break;
      case VALUE_NUMBER_INT:
        if (allTextMode) {
          handleString(parser, list);
          break;
        }
        BigIntHolder bh = new BigIntHolder();
        bh.value = parser.getLongValue();
        list.bigInt().write(bh);
        break;
      case VALUE_STRING:
        handleString(parser, list);
        break;
      default:
        throw new IllegalStateException("Unexpected token " + parser.getCurrentToken());
      }
    }
    list.end();


  }
}
