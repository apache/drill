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
package org.apache.drill.exec.store.ischema;

import java.util.ArrayList;
import java.util.ListIterator;

/**
 * PipeProvider sets up the framework so some subclass can "write" rows
 * to a an internal pipe, which another class (RowRecordReader) can "read" to
 * build up a record batch.
 * <p>
 * This class helps work around the situation where the rows cannot be conveniently
 * be generated one at a time by an iterator. Logically, the "writer" writes rows to the pipe,
 * while a "reader" reads rows from the pipe. The vocabulary implies two separate threads,
 * but the current implementation is actually just a wrapper around a List.
 */
public abstract class PipeProvider implements RowProvider {
  ArrayList<Object[]> pipe = null;
  ListIterator<Object[]> iter;
  
  /**
   * Method to generate and write rows to the pipe.
   */
  abstract void generateRows();
  
  /**
   * true if there are rows waiting to be "read".
   */
  public boolean hasNext() {
    if (pipe == null) {
      pipe = new ArrayList<Object[]>();
      generateRows();
      iter = pipe.listIterator();
    }
    return iter.hasNext();
  }
  
  /**
   * Read the next row from the pipe. 
   * Should only be called after "hasNext" indicates there are more rows.
   */
  public Object[] next() {
    return iter.next();
  }
  
  /**
   * Sometimes, a row cannot be immediately processed. Put the last row back and re-read it next time.
   */
  public void previous() {
    iter.previous();
  }
  
  /**
   * Write a row to the pipe.
   * @param values - a varargs list of values, in the same order as the RecordReader's value vectors.
   * @return true if the row was successfully written to the pipe.
   */
  protected boolean writeRow(Object...values) {
    pipe.add(values);
    return true;
  }
  
}
  