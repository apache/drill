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
package org.apache.drill.exec.ref.rops;

import static org.junit.Assert.*;

import org.apache.drill.exec.ref.*;
import org.apache.drill.exec.ref.eval.BasicEvaluatorFactory;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import org.apache.drill.common.logical.data.Limit;


public class LimitROPTest {
  final String input = "" +
      "{id: 1, name: \"jim\"}" +
      "{id: 2, name: \"bob\"}" +
      "{id: 3, name: \"larry\"}" +
      "{id: 4, name: \"fred\"}";

  private LimitROP buildROP(Limit l) {
    LimitROP limitROP = new LimitROP(l);
    limitROP.setupEvals(new BasicEvaluatorFactory(new IteratorRegistry()));
    return limitROP;
  }

  @Test
  public void limitStart() throws IOException {
    RecordIterator incoming = TestUtils.jsonToRecordIterator("test", input);
    LimitROP limitROP = buildROP(new Limit(0, 2));
    limitROP.setInput(incoming);

    int nRecords = TestUtils.getIteratorCount(limitROP.getOutput());
    assertEquals(2, nRecords);
  }

  @Test
  public void limitMiddle() throws IOException {
    RecordIterator incoming = TestUtils.jsonToRecordIterator("test", input);
    LimitROP limitROP = buildROP(new Limit(1, 2));
    limitROP.setInput(incoming);

    int nRecords = TestUtils.getIteratorCount(limitROP.getOutput());
    assertEquals(1, nRecords);
  }

  @Test
  public void limitEnd() throws IOException {
    RecordIterator incoming = TestUtils.jsonToRecordIterator("test", input);
    LimitROP limitROP = buildROP(new Limit(3, 10));
    limitROP.setInput(incoming);

    int nRecords = TestUtils.getIteratorCount(limitROP.getOutput());
    assertEquals(1, nRecords);
  }

  @Test
  public void limitZeroLength() throws IOException {
    RecordIterator incoming = TestUtils.jsonToRecordIterator("test", input);
    LimitROP limitROP = buildROP(new Limit(1, 1));
    limitROP.setInput(incoming);

    int nRecords = TestUtils.getIteratorCount(limitROP.getOutput());
    assertEquals(0, nRecords);
  }
}
