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
package org.apache.drill.common.logical.data;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.apache.drill.common.logical.data.Sequence.De;
import org.apache.drill.common.logical.data.visitors.LogicalVisitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.annotation.ObjectIdGenerator;
import com.fasterxml.jackson.annotation.ObjectIdGenerators;
import com.fasterxml.jackson.core.JsonLocation;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.deser.impl.ReadableObjectId;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.google.common.collect.Iterators;

// TODO - is this even ever used anymore? I don't believe the planner will ever
// generate this, we might have some tests with old logical plans that use this
// but it should probably be removed
/**
 * Describes a list of operators where each operator only has one input and that
 * input is the operator that came before.
 *
 */
@Deprecated
@JsonDeserialize(using = De.class)
@JsonTypeName("sequence")
public class Sequence extends LogicalOperatorBase {
  static final Logger logger = LoggerFactory.getLogger(Sequence.class);

  private Sequence() {}

  public boolean openTop;
  public LogicalOperator input;
  @JsonProperty("do")
  public List<LogicalOperator> stream;

    @Override
    public <T, X, E extends Throwable> T accept(LogicalVisitor<T, X, E> logicalVisitor, X value) throws E {
        return logicalVisitor.visitSequence(this, value);
    }

    @Override
    public Iterator<LogicalOperator> iterator() {
        return Iterators.singletonIterator(stream.get(stream.size() - 1));
    }

    public static class De extends StdDeserializer<LogicalOperator> {

    protected De() {
      super(Sequence.class);
    }

    @Override
    public LogicalOperator deserialize(JsonParser jp, DeserializationContext ctxt) throws IOException,
        JsonProcessingException {
      ObjectIdGenerator<Integer> idGenerator = new ObjectIdGenerators.IntSequenceGenerator();
      JsonLocation start = jp.getCurrentLocation();
      JsonToken t = jp.getCurrentToken();
      LogicalOperator parent = null;
      LogicalOperator first = null;
      LogicalOperator prev = null;
      Integer id = null;

      while (true) {
        String fieldName = jp.getText();
        t = jp.nextToken();
        switch (fieldName) { // switch on field names.
        case "@id":
          id = _parseIntPrimitive(jp, ctxt);
          break;
        case "input":
          JavaType tp = ctxt.constructType(LogicalOperator.class);
          JsonDeserializer<Object> d = ctxt.findRootValueDeserializer(tp);
          parent = (LogicalOperator) d.deserialize(jp, ctxt);
          break;

        case "do":
          if (!jp.isExpectedStartArrayToken()) {
            throwE(
                jp,
                "The do parameter of sequence should be an array of SimpleOperators.  Expected a JsonToken.START_ARRAY token but received a "
                    + t.name() + "token.");
          }

          int pos = 0;
          while ((t = jp.nextToken()) != JsonToken.END_ARRAY) {
            // logger.debug("Reading sequence child {}.", pos);
            JsonLocation l = jp.getCurrentLocation(); // get current location
                                                      // first so we can
                                                      // correctly reference the
                                                      // start of the object in
                                                      // the case that the type
                                                      // is wrong.
            LogicalOperator o = jp.readValueAs(LogicalOperator.class);

            if (pos == 0) {
              if (!(o instanceof SingleInputOperator) && !(o instanceof SourceOperator)) {
                throwE(
                    l,
                    "The first operator in a sequence must be either a ZeroInput or SingleInput operator.  The provided first operator was not. It was of type "
                        + o.getClass().getName());
              }
              first = o;
            } else {
              if (!(o instanceof SingleInputOperator)) {
                throwE(l, "All operators after the first must be single input operators.  The operator at position "
                    + pos + " was not. It was of type " + o.getClass().getName());
              }
              SingleInputOperator now = (SingleInputOperator) o;
              now.setInput(prev);
            }
            prev = o;

            pos++;
          }
          break;
        default:
          throwE(jp, "Unknown field name provided for Sequence: " + jp.getText());
        }

        t = jp.nextToken();
        if (t == JsonToken.END_OBJECT) {
          break;
        }
      }

      if (first == null) {
        throwE(start, "A sequence must include at least one operator.");
      }
      if ((parent == null && first instanceof SingleInputOperator)
          || (parent != null && first instanceof SourceOperator)) {
        throwE(start,
            "A sequence must either start with a ZeroInputOperator or have a provided input. It cannot have both or neither.");
      }

      if (parent != null && first instanceof SingleInputOperator) {
        ((SingleInputOperator) first).setInput(parent);
      }

      // set input reference.
      if (id != null) {

        ReadableObjectId rid = ctxt.findObjectId(id, idGenerator);
        rid.bindItem(prev);
        // logger.debug("Binding id {} to item {}.", rid.id, rid.item);

      }

      return first;
    }

  }

  private static void throwE(JsonLocation l, String e) throws JsonParseException {
    throw new JsonParseException(e, l);
  }

  private static void throwE(JsonParser jp, String e) throws JsonParseException {
    throw new JsonParseException(e, jp.getCurrentLocation());
  }

}
