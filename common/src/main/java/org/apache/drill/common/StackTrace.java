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
package org.apache.drill.common;

import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.util.Arrays;

/**
 * Convenient way of obtaining and manipulating stack traces for debugging.
 */
public class StackTrace {
  private final StackTraceElement[] stackTraceElements;

  /**
   * Constructor. Captures the current stack trace.
   */
  public StackTrace() {
    // skip over the first element so that we don't include this constructor call
    final StackTraceElement[] stack = Thread.currentThread().getStackTrace();
    stackTraceElements = Arrays.copyOfRange(stack, 1, stack.length - 1);
  }

  /**
   * Write the stack trace.
   *
   * @param writer where to write it
   * @param indent how many spaces to indent each line
   */
  public void write(final Writer writer, final int indent) {
    // create the indentation string
    final char[] indentation = new char[indent];
    Arrays.fill(indentation, ' ');

    try {
      // write the stack trace
      for(StackTraceElement ste : stackTraceElements) {
        writer.write(indentation);
        writer.write(ste.getClassName());
        writer.write('.');
        writer.write(ste.getMethodName());
        writer.write(':');
        writer.write(Integer.toString(ste.getLineNumber()));
        writer.write('\n');
      }
    } catch(IOException e) {
      throw new RuntimeException("couldn't write", e);
    }
  }

  @Override
  public String toString() {
    final StringWriter sw = new StringWriter();
    write(sw, 0);
    return sw.toString();
  }
}
