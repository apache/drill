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
package org.apache.drill.exec.vector.complex.fn;

import org.apache.drill.shaded.guava.com.google.common.collect.Lists;
import org.apache.drill.common.expression.PathSegment;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.vector.complex.writer.BaseWriter;

import java.util.BitSet;
import java.util.Collection;
import java.util.List;

public class JsonReaderUtils {

  public static void ensureAtLeastOneField(BaseWriter.ComplexWriter writer,
                                    Collection<SchemaPath> columns,
                                    boolean allTextMode,
                                    List<BaseWriter.ListWriter> emptyArrayWriters) {

    List<BaseWriter.MapWriter> writerList = Lists.newArrayList();
    List<PathSegment> fieldPathList = Lists.newArrayList();
    BitSet emptyStatus = new BitSet(columns.size());
    int i = 0;

    // first pass: collect which fields are empty
    for (SchemaPath sp : columns) {
      PathSegment fieldPath = sp.getRootSegment();
      BaseWriter.MapWriter fieldWriter = writer.rootAsMap();
      while (fieldPath.getChild() != null && !fieldPath.getChild().isArray()) {
        fieldWriter = fieldWriter.map(fieldPath.getNameSegment().getPath());
        fieldPath = fieldPath.getChild();
      }
      writerList.add(fieldWriter);
      fieldPathList.add(fieldPath);
      if (fieldWriter.isEmptyMap()) {
        emptyStatus.set(i, true);
      }
      if (i == 0 && !allTextMode) {
        // when allTextMode is false, there is not much benefit to producing all
        // the empty fields; just produce 1 field. The reason is that the type of the
        // fields is unknown, so if we produce multiple Integer fields by default, a
        // subsequent batch that contains non-integer fields will error out in any case.
        // Whereas, with allTextMode true, we are sure that all fields are going to be
        // treated as varchar, so it makes sense to produce all the fields, and in fact
        // is necessary in order to avoid schema change exceptions by downstream operators.
        break;
      }
      i++;
    }

    // second pass: create default typed vectors corresponding to empty fields
    // Note: this is not easily do-able in 1 pass because the same fieldWriter
    // may be shared by multiple fields whereas we want to keep track of all fields
    // independently, so we rely on the emptyStatus.
    for (int j = 0; j < fieldPathList.size(); j++) {
      BaseWriter.MapWriter fieldWriter = writerList.get(j);
      PathSegment fieldPath = fieldPathList.get(j);
      if (emptyStatus.get(j)) {
        if (allTextMode) {
          fieldWriter.varChar(fieldPath.getNameSegment().getPath());
        } else {
          fieldWriter.integer(fieldPath.getNameSegment().getPath());
        }
      }
    }

    for (BaseWriter.ListWriter field : emptyArrayWriters) {
      // checks that array has not been initialized
      if (field.getValueCapacity() == 0) {
        if (allTextMode) {
          field.varChar();
        } else {
          field.integer();
        }
      }
    }
  }
}