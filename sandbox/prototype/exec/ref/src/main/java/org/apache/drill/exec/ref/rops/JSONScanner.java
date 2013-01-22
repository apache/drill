/*******************************************************************************
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
 ******************************************************************************/
package org.apache.drill.exec.ref.rops;

import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Iterator;

import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.logical.data.Scan;
import org.apache.drill.exec.ref.RecordIterator;
import org.apache.drill.exec.ref.RecordPointer;
import org.apache.drill.exec.ref.UnbackedRecord;
import org.apache.drill.exec.ref.exceptions.RecordException;
import org.apache.drill.exec.ref.values.DataValue;
import org.apache.drill.exec.ref.values.SimpleArrayValue;
import org.apache.drill.exec.ref.values.SimpleMapValue;
import org.apache.drill.exec.ref.values.ScalarValues.BooleanScalar;
import org.apache.drill.exec.ref.values.ScalarValues.BytesScalar;
import org.apache.drill.exec.ref.values.ScalarValues.DoubleScalar;
import org.apache.drill.exec.ref.values.ScalarValues.IntegerScalar;
import org.apache.drill.exec.ref.values.ScalarValues.LongScalar;
import org.apache.drill.exec.ref.values.ScalarValues.StringScalar;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonParser.Feature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;

public class JSONScanner extends ROPBase<Scan> {
  private static final Logger logger = LoggerFactory.getLogger(JSONScanner.class);

  private ObjectMapper mapper = new ObjectMapper();
  private InputStreamReader input;
  private String file;
  private SchemaPath rootPath;
  private JsonParser parser;
  private UnbackedRecord record = new UnbackedRecord();

  public JSONScanner(Scan scan, String file) throws IOException {
    super(scan);
    JsonFactory factory = new JsonFactory();
    factory.configure(Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);
    factory.configure(Feature.ALLOW_COMMENTS, true);
    FileSystem fs = FileSystem.getLocal(new Configuration());
    if(!file.contains("://")){
      String current = new java.io.File( "." ).getCanonicalPath();
      file = "file://" + current + "/" + file;
    }
    FSDataInputStream stream = fs.open(new Path(file));
    this.input = new InputStreamReader(stream, Charsets.UTF_8);
    this.parser = factory.createJsonParser(fs.open(new Path(file)));
    this.file = file;
    this.rootPath = config.getOutputReference();
  }

  private class NodeIter implements RecordIterator {

    @Override
    public NextOutcome next() {
//      logger.debug("Next Record Called");
      try {
        if (parser.nextToken() == null) {
//          logger.debug("No current token, returning.");
          return NextOutcome.NONE_LEFT;
        }
        JsonNode n = mapper.readTree(parser);
        if (n == null) {
//          logger.debug("Nothing was returned for read tree, returning.");
          return NextOutcome.NONE_LEFT;
        }
//        logger.debug("Record found, returning new json record.");
        record.setClearAndSetRoot(rootPath, convert(n));
        // todo, add schema checking here.
        return NextOutcome.INCREMENTED_SCHEMA_CHANGED;
      } catch (IOException e) {
        throw new RecordException("Failure while reading record", null, e);
      }
    }

    @Override
    public ROP getParent() {
      return JSONScanner.this;
    }

    @Override
    public RecordPointer getRecordPointer() {
      return record;
    }

  }

  public static DataValue convert(JsonNode node) {
    if (node == null || node.isNull() || node.isMissingNode()) {
      return DataValue.NULL_VALUE;
    } else if (node.isArray()) {
      SimpleArrayValue arr = new SimpleArrayValue(node.size());
      for (int i = 0; i < node.size(); i++) {
        arr.addToArray(i, convert(node.get(i)));
      }
      return arr;
    } else if (node.isObject()) {
      SimpleMapValue map = new SimpleMapValue();
      String name;
      for (Iterator<String> iter = node.fieldNames(); iter.hasNext();) {
        name = iter.next();
        map.setByName(name, convert(node.get(name)));
      }
      return map;
    } else if (node.isBinary()) {
      try {
        return new BytesScalar(node.binaryValue());
      } catch (IOException e) {
        throw new RuntimeException("Failure converting binary value.", e);
      }
    } else if (node.isBigDecimal()) {
      throw new UnsupportedOperationException();
//      return new BigDecimalScalar(node.decimalValue());
    } else if (node.isBigInteger()) {
      throw new UnsupportedOperationException();
//      return new BigIntegerScalar(node.bigIntegerValue());
    } else if (node.isBoolean()) {
      return new BooleanScalar(node.asBoolean());
    } else if (node.isFloatingPointNumber()) {
      if (node.isBigDecimal()) {
        throw new UnsupportedOperationException();
//        return new BigDecimalScalar(node.decimalValue());
      } else {
        return new DoubleScalar(node.asDouble());
      }
    } else if (node.isInt()) {
      return new IntegerScalar(node.asInt());
    } else if (node.isLong()) {
      return new LongScalar(node.asLong());
    } else if (node.isTextual()) {
      return new StringScalar(node.asText());
    } else {
      throw new UnsupportedOperationException(String.format("Don't know how to convert value of type %s.", node
          .getClass().getCanonicalName()));
    }

  }

  @Override
  protected RecordIterator getIteratorInternal() {
    return new NodeIter();
  }

  @Override
  public void cleanup() {
    try {
      parser.close();
      this.input.close();
    } catch (IOException e) {
      logger.warn("Error while closing InputStream for file {}", file, e);
    }

  }

}
