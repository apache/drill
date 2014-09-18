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
package org.apache.drill.common.expression;

import java.io.IOException;

import org.antlr.runtime.ANTLRStringStream;
import org.antlr.runtime.CommonTokenStream;
import org.antlr.runtime.RecognitionException;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.expression.parser.ExprLexer;
import org.apache.drill.common.expression.parser.ExprParser;
import org.apache.drill.common.expression.parser.ExprParser.parse_return;
import org.apache.drill.common.expression.visitors.ExprVisitor;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;

//@JsonDeserialize(using = LogicalExpression.De.class)  // Excluded as we need to register this with the DrillConfig.
@JsonSerialize(using = LogicalExpression.Se.class)
public interface LogicalExpression extends Iterable<LogicalExpression>{
  static final Logger logger = LoggerFactory.getLogger(LogicalExpression.class);

  public abstract MajorType getMajorType();

  public <T, V, E extends Exception> T accept(ExprVisitor<T, V, E> visitor, V value) throws E;

  public ExpressionPosition getPosition();

  public int getSelfCost();
  public int getCumulativeCost();

  public static class De extends StdDeserializer<LogicalExpression> {
    DrillConfig config;

    public De(DrillConfig config) {
      super(LogicalExpression.class);
      this.config = config;
    }

    @Override
    public LogicalExpression deserialize(JsonParser jp, DeserializationContext ctxt) throws IOException,
        JsonProcessingException {
      String expr = jp.getText();

      if (expr == null || expr.isEmpty()) {
        return null;
      }
      try {
        // logger.debug("Parsing expression string '{}'", expr);
        ExprLexer lexer = new ExprLexer(new ANTLRStringStream(expr));
        CommonTokenStream tokens = new CommonTokenStream(lexer);
        ExprParser parser = new ExprParser(tokens);

        //TODO: move functionregistry and error collector to injectables.
        //ctxt.findInjectableValue(valueId, forProperty, beanInstance)
        parse_return ret = parser.parse();

        // ret.e.resolveAndValidate(expr, errorCollector);
        return ret.e;
      } catch (RecognitionException e) {
        throw new RuntimeException(e);
      }
    }

  }

  public static class Se extends StdSerializer<LogicalExpression> {

    protected Se() {
      super(LogicalExpression.class);
    }

    @Override
    public void serialize(LogicalExpression value, JsonGenerator jgen, SerializerProvider provider) throws IOException,
        JsonGenerationException {
      StringBuilder sb = new StringBuilder();
      ExpressionStringBuilder esb = new ExpressionStringBuilder();
      value.accept(esb, sb);
      jgen.writeString(sb.toString());
    }

  }

}
