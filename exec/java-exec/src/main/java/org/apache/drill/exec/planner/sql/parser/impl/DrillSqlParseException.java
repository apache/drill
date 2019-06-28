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
package org.apache.drill.exec.planner.sql.parser.impl;

import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParserPos;

/**
 * Customized {@link SqlParseException} class
 */
public class DrillSqlParseException extends SqlParseException {
  private final ParseException parseException;

  public DrillSqlParseException(String message, SqlParserPos pos, int[][] expectedTokenSequences,
                                String[] tokenImages, Throwable ex) {
    super(message, pos, expectedTokenSequences, tokenImages, ex);

    parseException = (ex instanceof ParseException) ? (ParseException) ex : null;
  }

  public DrillSqlParseException(SqlParseException sqlParseException) {
    this(sqlParseException.getMessage(), sqlParseException.getPos(), sqlParseException.getExpectedTokenSequences(),
        sqlParseException.getTokenImages(), sqlParseException.getCause());
  }

  /**
   * Builds error message just like the original {@link SqlParseException}
   * with special handling for {@link ParseException} class.
   * <p>
   * This is customized implementation of the original {@link ParseException#getMessage()}.
   * Any other underlying {@link SqlParseException} exception messages goes through without changes
   * <p>
   * <p>
   * Example:
   * <pre>
   *
   *   Given query: SELECT FROM (VALUES(1));
   *
   *   Generated message for the unsupported FROM token after SELECT would look like:
   *
   *       Encountered "FROM" at line 1, column 8.
   *</pre>
   * @return formatted string representation of {@link DrillSqlParseException}
   */
  @Override
  public String getMessage() {
    // proxy the original message if exception does not belongs
    // to ParseException or no current token passed
    if (parseException == null || parseException.currentToken == null) {
      return super.getMessage();
    }

    int[][] expectedTokenSequences = getExpectedTokenSequences();
    String[] tokenImage = getTokenImages();

    int maxSize = 0;  // holds max possible length of the token sequence
    for (int[] expectedTokenSequence : expectedTokenSequences) {
      if (maxSize < expectedTokenSequence.length) {
        maxSize = expectedTokenSequence.length;
      }
    }

    // parseException.currentToken points to the last successfully parsed token, next one is considered as fail reason
    Token tok = parseException.currentToken.next;
    StringBuilder sb = new StringBuilder("Encountered \"");

    // Adds unknown token sequences to the message
    for (int i = 0; i < maxSize; i++) {
      if (i != 0) {
        sb.append(" ");
      }

      if (tok.kind == DrillParserImplConstants.EOF) {
        sb.append(tokenImage[0]);
        break;
      }
      sb.append(parseException.add_escapes(tok.image));
      tok = tok.next;
    }

    sb
        .append("\" at line ")
        .append(parseException.currentToken.beginLine)
        .append(", column ")
        .append(parseException.currentToken.next.beginColumn)
        .append(".");

    return sb.toString();
  }
}
