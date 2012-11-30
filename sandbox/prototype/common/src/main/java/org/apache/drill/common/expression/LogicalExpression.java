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
package org.apache.drill.common.expression;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import org.antlr.runtime.ANTLRStringStream;
import org.antlr.runtime.CommonTokenStream;
import org.antlr.runtime.RecognitionException;
import org.apache.drill.common.expression.parser.ExprLexer;
import org.apache.drill.common.expression.parser.ExprParser;
import org.apache.drill.common.expression.parser.ExprParser.parse_return;
import org.apache.drill.common.expression.visitors.FunctionVisitor;
import org.apache.drill.common.logical.ValidationError;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;

@JsonDeserialize(using = LogicalExpression.De.class)
@JsonSerialize(using = LogicalExpression.Se.class)
// @JsonTypeInfo(use = JsonTypeInfo.Id.MINIMAL_CLASS, include =
// JsonTypeInfo.As.PROPERTY, property = "fn")
public interface LogicalExpression {
  static final Logger logger = LoggerFactory.getLogger(LogicalExpression.class);

  public static final Class<?>[] SUB_TYPES = {};

  LogicalExpression wrapWithCastIfNecessary(DataType dt) throws ExpressionValidationError;

  @JsonIgnore
  public abstract DataType getDataType();

  public void addToString(StringBuilder sb);

  public void resolveAndValidate(List<LogicalExpression> expressions, Collection<ValidationError> errors);

  public Object accept(FunctionVisitor visitor);

  public static class De extends StdDeserializer<LogicalExpression> {

    public De() {
      super(LogicalExpression.class);
    }

    @Override
    public LogicalExpression deserialize(JsonParser jp, DeserializationContext ctxt) throws IOException,
        JsonProcessingException {
      String expr = jp.getText();

      if (expr == null || expr.isEmpty())
        return null;
      try {
        // logger.debug("Parsing expression string '{}'", expr);
        ExprLexer lexer = new ExprLexer(new ANTLRStringStream(expr));

        CommonTokenStream tokens = new CommonTokenStream(lexer);
        ExprParser parser = new ExprParser(tokens);
        parse_return ret = parser.parse();
        // logger.debug("Found expression '{}'", ret.e);
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
      value.addToString(sb);
      jgen.writeString(sb.toString());
    }

  }

}
