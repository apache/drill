/*******************************************************************************
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
 ******************************************************************************/
package org.apache.drill.hbase;

import org.apache.drill.common.expression.PathSegment;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.ref.RecordPointer;
import org.apache.drill.exec.ref.rops.DataWriter;
import org.apache.drill.exec.ref.values.DataValue;
import org.apache.drill.hbase.values.HBaseResultValue;
import org.apache.hadoop.hbase.client.Result;

import java.io.IOException;

/**
 * A pointer to a single HBase result, i.e. a row.
 */
public class HBaseResultRecordPointer implements RecordPointer {

  private HBaseResultValue result;

  public HBaseResultRecordPointer() {
  }

  private HBaseResultRecordPointer(HBaseResultValue result) {
    this.result = result;
  }

  public void clearAndSet(SchemaPath rootPath, Result result) {
    this.result = new HBaseResultValue(result);
  }

  @Override
  public DataValue getField(SchemaPath field) {
    return this.result.getValue(field.getRootSegment());
  }

  @Override
  public void addField(SchemaPath field, DataValue value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void addField(PathSegment segment, DataValue value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void removeField(SchemaPath segment) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void write(DataWriter writer) throws IOException {
    writer.startRecord();
    result.write(writer);
    writer.endRecord();
  }

  @Override
  public HBaseResultRecordPointer copy() {
    return new HBaseResultRecordPointer(this.result);
  }

  @Override
  public void copyFrom(RecordPointer r) {
    if (HBaseResultRecordPointer.class.isAssignableFrom(r.getClass())) {
      this.result = ((HBaseResultRecordPointer) r).result;
    }
  }

  @Override
  public String toString() {
    return "HBaseResultRecordPointer{" +
      "result=" + result +
      '}';
  }
}
