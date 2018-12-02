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

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class TestParserErrorHandling {

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testUnsupportedType() {
    String schema = "col unk_type";
    thrown.expect(SchemaParsingException.class);
    thrown.expectMessage("offending symbol [@1,4:11='unk_type',<38>,1:4]: no viable alternative at input");
    SchemaExprParser.parseSchema(schema);
  }

  @Test
  public void testVarcharWithScale() {
    String schema = "col varchar(1, 2)";
    thrown.expect(SchemaParsingException.class);
    thrown.expectMessage("missing ')' at ','");
    SchemaExprParser.parseSchema(schema);
  }

  @Test
  public void testUnquotedKeyword() {
    String schema = "int varchar";
    thrown.expect(SchemaParsingException.class);
    thrown.expectMessage("mismatched input 'int' expecting {'(', ID, QUOTED_ID}");
    SchemaExprParser.parseSchema(schema);
  }

  @Test
  public void testUnquotedId() {
    String schema = "id with space varchar";
    thrown.expect(SchemaParsingException.class);
    thrown.expectMessage("offending symbol [@1,3:6='with',<38>,1:3]: no viable alternative at input");
    SchemaExprParser.parseSchema(schema);
  }

  @Test
  public void testUnescapedBackTick() {
    String schema = "`c`o`l` varchar";
    thrown.expect(SchemaParsingException.class);
    thrown.expectMessage("offending symbol [@1,3:3='o',<38>,1:3]: no viable alternative at input");
    SchemaExprParser.parseSchema(schema);
  }

  @Test
  public void testUnescapedBackSlash() {
    String schema = "`c\\o\\l` varchar";
    thrown.expect(SchemaParsingException.class);
    thrown.expectMessage("extraneous input '`' expecting {'(', ID, QUOTED_ID}");
    SchemaExprParser.parseSchema(schema);
  }

  @Test
  public void testMissingType() {
    String schema = "col not null";
    thrown.expect(SchemaParsingException.class);
    thrown.expectMessage("offending symbol [@1,4:6='not',<34>,1:4]: no viable alternative at input");
    SchemaExprParser.parseSchema(schema);
  }

  @Test
  public void testIncorrectEOF() {
    String schema = "col int not null footer";
    thrown.expect(SchemaParsingException.class);
    thrown.expectMessage("extraneous input 'footer' expecting <EOF>");
    SchemaExprParser.parseSchema(schema);
  }

  @Test
  public void testSchemaWithOneParen() {
    String schema = "(col int not null";
    thrown.expect(SchemaParsingException.class);
    thrown.expectMessage("missing ')' at '<EOF>'");
    SchemaExprParser.parseSchema(schema);
  }

  @Test
  public void testMissingAngleBracket() {
    String schema = "col array<int not null";
    thrown.expect(SchemaParsingException.class);
    thrown.expectMessage("missing '>' at 'not'");
    SchemaExprParser.parseSchema(schema);
  }

  @Test
  public void testUnclosedAngleBracket() {
    String schema = "col map<m array<int> not null";
    thrown.expect(SchemaParsingException.class);
    thrown.expectMessage("missing '>' at '<EOF>'");
    SchemaExprParser.parseSchema(schema);
  }

  @Test
  public void testMissingColumnNameForMap() {
    String schema = "col map<int> not null";
    thrown.expect(SchemaParsingException.class);
    thrown.expectMessage("mismatched input 'int' expecting {ID, QUOTED_ID}");
    SchemaExprParser.parseSchema(schema);
  }

  @Test
  public void testMissingNotBeforeNull() {
    String schema = "col int null";
    thrown.expect(SchemaParsingException.class);
    thrown.expectMessage("extraneous input 'null' expecting <EOF>");
    SchemaExprParser.parseSchema(schema);
  }

  @Test
  public void testExtraComma() {
    String schema = "id int,, name varchar";
    thrown.expect(SchemaParsingException.class);
    thrown.expectMessage("extraneous input ',' expecting {ID, QUOTED_ID}");
    SchemaExprParser.parseSchema(schema);
  }

  @Test
  public void testExtraCommaEOF() {
    String schema = "id int, name varchar,";
    thrown.expect(SchemaParsingException.class);
    thrown.expectMessage("mismatched input '<EOF>' expecting {ID, QUOTED_ID}");
    SchemaExprParser.parseSchema(schema);
  }

  @Test
  public void incorrectNumber() {
    String schema = "id decimal(5, 02)";
    thrown.expect(SchemaParsingException.class);
    thrown.expectMessage("extraneous input '2' expecting ')'");
    SchemaExprParser.parseSchema(schema);
  }

}
