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
package org.apache.drill.exec.store.pojo;

import com.google.common.base.Preconditions;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.exec.physical.impl.OutputMutator;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Dynamically reads values from the given list of records.
 * Creates writers based on given schema.
 *
 * @param <T> type of given values, if contains various types, use Object class
 */
public class DynamicPojoRecordReader<T> extends AbstractPojoRecordReader<List<T>> {

  private final LinkedHashMap<String, Class<?>> schema;

  public DynamicPojoRecordReader(LinkedHashMap<String, Class<?>> schema, List<List<T>> records) {
    super(records);
    Preconditions.checkState(schema != null && !schema.isEmpty(), "Undefined schema is not allowed.");
    this.schema = schema;
  }

  /**
   * Initiates writers based on given schema which contains field name and its type.
   *
   * @param output output mutator
   * @return list of pojo writers
   */
  @Override
  protected List<PojoWriter> setupWriters(OutputMutator output) throws ExecutionSetupException {
    List<PojoWriter> writers = new ArrayList<>();
    for (Map.Entry<String, Class<?>> field : schema.entrySet()) {
      writers.add(initWriter(field.getValue(), field.getKey(), output));
    }
    return writers;
  }

  @Override
  protected Object getFieldValue(List<T> row, int fieldPosition) {
    return row.get(fieldPosition);
  }

  @Override
  public String toString() {
    return "DynamicPojoRecordReader{" +
        "records = " + records +
        "}";
  }
}
