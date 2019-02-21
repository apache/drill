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
package org.apache.drill.exec.record.metadata.schema.parser;

import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CodePointCharStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;
import org.apache.drill.exec.record.metadata.ColumnMetadata;
import org.apache.drill.exec.record.metadata.TupleMetadata;

public class SchemaExprParser {

  /**
   * Parses string definition of the schema and converts it
   * into {@link TupleMetadata} instance.
   *
   * @param schema schema definition
   * @return metadata description of the schema
   */
  public static TupleMetadata parseSchema(String schema) {
    SchemaVisitor visitor = new SchemaVisitor();
    return visitor.visit(initParser(schema).schema());
  }

  /**
   * Parses string definition of the column and converts it
   * into {@link ColumnMetadata} instance.
   *
   * @param column column definition
   * @return metadata description of the column
   */
  public static ColumnMetadata parseColumn(String column) {
    SchemaVisitor.ColumnVisitor visitor = new SchemaVisitor.ColumnVisitor();
    return visitor.visit(initParser(column).column());
  }

  private static SchemaParser initParser(String value) {
    CodePointCharStream stream = CharStreams.fromString(value);
    UpperCaseCharStream upperCaseStream = new UpperCaseCharStream(stream);

    SchemaLexer lexer = new SchemaLexer(upperCaseStream);
    lexer.removeErrorListeners();
    lexer.addErrorListener(ErrorListener.INSTANCE);

    CommonTokenStream tokens = new CommonTokenStream(lexer);

    SchemaParser parser = new SchemaParser(tokens);
    parser.removeErrorListeners();
    parser.addErrorListener(ErrorListener.INSTANCE);

    return parser;
  }

  /**
   * Custom error listener that converts all syntax errors into {@link SchemaParsingException}.
   */
  private static class ErrorListener extends BaseErrorListener {

    static final ErrorListener INSTANCE = new ErrorListener();

    @Override
    public void syntaxError(Recognizer<?, ?> recognizer, Object offendingSymbol, int line,
                            int charPositionInLine, String msg, RecognitionException e) {
      StringBuilder builder = new StringBuilder();
      builder.append("Line [").append(line).append("]");
      builder.append(", position [").append(charPositionInLine).append("]");
      if (offendingSymbol != null) {
        builder.append(", offending symbol ").append(offendingSymbol);
      }
      if (msg != null) {
        builder.append(": ").append(msg);
      }
      throw new SchemaParsingException(builder.toString());
    }

  }

}
