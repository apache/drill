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
package org.apache.drill.exec.ref.rse;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Iterator;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.ref.RecordIterator;
import org.apache.drill.exec.ref.RecordPointer;
import org.apache.drill.exec.ref.RunOutcome;
import org.apache.drill.exec.ref.UnbackedRecord;
import org.apache.drill.exec.ref.exceptions.RecordException;
import org.apache.drill.exec.ref.rops.ROP;
import org.apache.drill.exec.ref.values.DataValue;
import org.apache.drill.exec.ref.values.ScalarValues.BooleanScalar;
import org.apache.drill.exec.ref.values.ScalarValues.BytesScalar;
import org.apache.drill.exec.ref.values.ScalarValues.DoubleScalar;
import org.apache.drill.exec.ref.values.ScalarValues.IntegerScalar;
import org.apache.drill.exec.ref.values.ScalarValues.LongScalar;
import org.apache.drill.exec.ref.values.ScalarValues.StringScalar;
import org.apache.drill.exec.ref.values.SimpleArrayValue;
import org.apache.drill.exec.ref.values.SimpleMapValue;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;

public class JSONRecordReader implements RecordReader {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(JSONRecordReader.class);

  private InputStreamReader input;
  private String file;
  private SchemaPath rootPath;
  private JsonParser parser;
  private UnbackedRecord record = new UnbackedRecord();
  private ObjectMapper mapper;
  private ROP parent;

  public JSONRecordReader(SchemaPath rootPath, DrillConfig dConfig, InputStream stream, ROP parent) throws IOException {
    this.input = new InputStreamReader(stream, Charsets.UTF_8);
    this.mapper = dConfig.getMapper();
    this.parser = mapper.getFactory().createJsonParser(input);
    this.parent = parent;
    this.rootPath = rootPath;
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
    public RecordPointer getRecordPointer() {
      return record;
    }


    @Override
    public ROP getParent() {
      return parent;
    }

  }

  private DataValue convert(JsonNode node) {
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

  
  /* (non-Javadoc)
   * @see org.apache.drill.exec.ref.rse.DataReader#getIterator()
   */
  @Override
  public RecordIterator getIterator() {
    return new NodeIter();
  }

  /* (non-Javadoc)
   * @see org.apache.drill.exec.ref.rse.DataReader#cleanup()
   */
  @Override
  public void cleanup() {
    try {
      parser.close();
      this.input.close();
    } catch (IOException e) {
      logger.warn("Error while closing InputStream for file {}", file, e);
    }

  }


  @Override
  public void setup() {
  }
}
