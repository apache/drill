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
package org.apache.drill.exec.store.easy.text.compliant;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.test.DrillTest;
import org.junit.Test;

import org.apache.drill.shaded.guava.com.google.common.base.Charsets;

public class TestHeaderBuilder extends DrillTest {

  @Test
  public void testEmptyHeader() {
    HeaderBuilder hb = new HeaderBuilder();
    hb.startBatch();
    try {
      hb.finishRecord();
    } catch (UserException e) {
      assertTrue(e.getMessage().contains("must define at least one header"));
    }

    hb = new HeaderBuilder();
    hb.startBatch();
    parse(hb,"");
    try {
      hb.finishRecord();
    } catch (UserException e) {
      assertTrue(e.getMessage().contains("must define at least one header"));
    }

    hb = new HeaderBuilder();
    hb.startBatch();
    parse(hb,"   ");
    validateHeader(hb, new String[] {"column_1"});

    hb = new HeaderBuilder();
    hb.startBatch();
    parse(hb,",");
    validateHeader(hb, new String[] {"column_1", "column_2"});

    hb = new HeaderBuilder();
    hb.startBatch();
    parse(hb," , ");
    validateHeader(hb, new String[] {"column_1", "column_2"});

    hb = new HeaderBuilder();
    hb.startBatch();
    parse(hb,"a,   ");
    validateHeader(hb, new String[] {"a", "column_2"});
  }

  @Test
  public void testWhiteSpace() {
    HeaderBuilder hb = new HeaderBuilder();
    hb.startBatch();
    parse(hb,"a");
    validateHeader(hb, new String[] {"a"});

    hb = new HeaderBuilder();
    hb.startBatch();
    parse(hb," a ");
    validateHeader(hb, new String[] {"a"});

    hb = new HeaderBuilder();
    hb.startBatch();
    parse(hb,"    a    ");
    validateHeader(hb, new String[] {"a"});

    hb = new HeaderBuilder();
    hb.startBatch();
    parse(hb,"a,b,c");
    validateHeader(hb, new String[] {"a","b","c"});

    hb = new HeaderBuilder();
    hb.startBatch();
    parse(hb," a , b ,  c ");
    validateHeader(hb, new String[] {"a","b","c"});
  }

  @Test
  public void testSyntax() {
    HeaderBuilder hb = new HeaderBuilder();
    hb.startBatch();
    parse(hb,"a_123");
    validateHeader(hb, new String[] {"a_123"});

    hb = new HeaderBuilder();
    hb.startBatch();
    parse(hb,"a_123_");
    validateHeader(hb, new String[] {"a_123_"});

    hb = new HeaderBuilder();
    hb.startBatch();
    parse(hb,"az09_");
    validateHeader(hb, new String[] {"az09_"});

    hb = new HeaderBuilder();
    hb.startBatch();
    parse(hb,"+");
    validateHeader(hb, new String[] {"column_1"});

    hb = new HeaderBuilder();
    hb.startBatch();
    parse(hb,"+,-");
    validateHeader(hb, new String[] {"column_1", "column_2"});

    hb = new HeaderBuilder();
    hb.startBatch();
    parse(hb,"+9a");
    validateHeader(hb, new String[] {"col_9a"});

    hb = new HeaderBuilder();
    hb.startBatch();
    parse(hb,"9a");
    validateHeader(hb, new String[] {"col_9a"});

    hb = new HeaderBuilder();
    hb.startBatch();
    parse(hb,"a+b");
    validateHeader(hb, new String[] {"a_b"});

    hb = new HeaderBuilder();
    hb.startBatch();
    parse(hb,"a_b");
    validateHeader(hb, new String[] {"a_b"});

    hb = new HeaderBuilder();
    hb.startBatch();
    parse(hb,"EXPR$0");
    validateHeader(hb, new String[] {"EXPR_0"});

    hb = new HeaderBuilder();
    hb.startBatch();
    parse(hb,"(_-^-_)");
    validateHeader(hb, new String[] {"col_______"});
  }

  @Test
  public void testUnicode() {
    HeaderBuilder hb = new HeaderBuilder();
    hb.startBatch();
    parse(hb,"Αθήνα");
    validateHeader(hb, new String[] {"Αθήνα"});

    hb = new HeaderBuilder();
    hb.startBatch();
    parse(hb,"Москва");
    validateHeader(hb, new String[] {"Москва"});

    hb = new HeaderBuilder();
    hb.startBatch();
    parse(hb,"Paris,Αθήνα,Москва");
    validateHeader(hb, new String[] {"Paris","Αθήνα","Москва"});
  }

  @Test
  public void testDuplicateNames() {
    testParser("a,a", new String[] {"a","a_2"});
    testParser("a,A", new String[] {"a","A_2"});
    // It ain't pretty, but it is unique...
    testParser("a,A,A_2", new String[] {"a","A_2", "A_2_2"});
    // Verify with non-ASCII characters
    testParser("Αθήνα,ΑθήνΑ", new String[] {"Αθήνα","ΑθήνΑ_2"});
  }

  private void testParser(String input, String[] expected) {
    HeaderBuilder hb = new HeaderBuilder();
    hb.startBatch();
    parse(hb,input);
    hb.finishRecord();
    validateHeader(hb, expected);
  }

  private void parse(HeaderBuilder hb, String input) {
    if (input == null) {
      return;
    }
    byte bytes[] = input.getBytes(Charsets.UTF_8);
    if (bytes.length == 0) {
      return;
    }
    int fieldIndex = -1;
    hb.startField(++fieldIndex);
    for (int i = 0; i < bytes.length; i++) {
      byte b = bytes[i];
      if (b == ',') {
        hb.endField();
        hb.startField(++fieldIndex);
      } else {
        hb.append(b);
      }
    }
    hb.endField();
  }

  private void validateHeader(HeaderBuilder hb, String[] expected) {
    String actual[] = hb.getHeaders();
    assertEquals(expected.length, actual.length);
    for (int i = 0; i < expected.length; i++) {
      assertEquals(expected[i], actual[i]);
    }
  }

}
