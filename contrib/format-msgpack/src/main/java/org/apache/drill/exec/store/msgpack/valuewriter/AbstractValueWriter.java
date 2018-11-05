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

import java.util.Stack;

import org.apache.drill.exec.store.msgpack.MsgpackReaderContext;
import org.apache.drill.exec.vector.complex.writer.BaseWriter.ListWriter;
import org.apache.drill.exec.vector.complex.writer.BaseWriter.MapWriter;
import org.apache.drill.shaded.guava.com.google.common.base.Joiner;

public abstract class AbstractValueWriter implements ValueWriter {
  protected static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(AbstractValueWriter.class);

  protected MsgpackReaderContext context;

  private Stack<String> stack = new Stack<>();

  public AbstractValueWriter() {
    super();
  }

  public void setup(MsgpackReaderContext context) {
    this.context = context;
    this.stack.removeAllElements();
    this.stack.push("root");
  }

  public void push(String fieldName) {
    stack.push(fieldName);
  }

  public void pop() {
    stack.pop();
  }

  public String printPath(String fieldName, MapWriter mapWriter, ListWriter listWriter) {
      return Joiner.on(".").join(stack);
  }
}
