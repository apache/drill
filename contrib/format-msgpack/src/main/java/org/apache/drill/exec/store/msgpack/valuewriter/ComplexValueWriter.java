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
package org.apache.drill.exec.store.msgpack.valuewriter;

import java.util.EnumMap;

import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.store.msgpack.MsgpackParsingException;
import org.apache.drill.exec.vector.complex.fn.FieldSelection;
import org.apache.drill.exec.vector.complex.writer.BaseWriter.ListWriter;
import org.apache.drill.exec.vector.complex.writer.BaseWriter.MapWriter;
import org.msgpack.value.Value;
import org.msgpack.value.ValueType;
import org.slf4j.helpers.MessageFormatter;

public abstract class ComplexValueWriter extends AbstractValueWriter {

  protected EnumMap<ValueType, AbstractValueWriter> valueWriterMap;

  public ComplexValueWriter(EnumMap<ValueType, AbstractValueWriter> valueWriterMap) {
    super();
    this.valueWriterMap = valueWriterMap;
  }

  protected void writeElement(Value value, MapWriter mapWriter, ListWriter listWriter, String fieldName,
      FieldSelection selection, MaterializedField schema) {
    if (logger.isDebugEnabled()) {
      if (mapWriter != null) {
        logger.debug("write type: '{}' value: '{}' into map at '{}.{}' target schema: '{}'", value.getValueType(),
            value, printPath(fieldName, mapWriter, listWriter), fieldName, schema);
      } else {
        logger.debug("write type: '{}' value: '{}' into list at '{}.[]' target schema: '{}'", value.getValueType(),
            value, printPath(fieldName, mapWriter, listWriter), schema);
      }
    }

    try {
      // if (!checkElementSchema(value, schema)) {
      // return MSG_RECORD_PARSE_ERROR;
      // }

      valueWriterMap.get(value.getValueType()).write(value, mapWriter, fieldName, listWriter, selection, schema);
    } catch (Exception e) {
      String message = null;
      if (mapWriter != null) {
        message = MessageFormatter
            .arrayFormat("failed to write type: '{}' value: '{}' into map at '{}.{}' target schema: '{}'\n",
                new Object[] { value.getValueType(), value, printPath(fieldName, mapWriter, listWriter), fieldName,
                    schema })
            .getMessage();
      } else {
        message = MessageFormatter
            .arrayFormat("failed to write type: '{}' value: '{}' into list at '{}.[]' target schema: '{}'\n",
                new Object[] { value.getValueType(), value, printPath(fieldName, mapWriter, listWriter), schema })
            .getMessage();
      }
      if (context.lenient) {
        context.warn(message, e);
      } else {
        throw new MsgpackParsingException(message, e);
      }
    }
  }
}
