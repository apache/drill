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
package org.apache.drill.exec.ref;

import java.io.IOException;

import org.apache.drill.common.expression.PathSegment;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.expression.ValueExpressions.CollisionBehavior;
import org.apache.drill.exec.ref.rops.DataWriter;
import org.apache.drill.exec.ref.values.ContainerValue;
import org.apache.drill.exec.ref.values.DataValue;
import org.apache.drill.exec.ref.values.SimpleMapValue;
import org.apache.drill.exec.ref.values.ValueUtils;

public class UnbackedRecord implements RecordPointer {

    private DataValue root = new SimpleMapValue();

    public DataValue getField(SchemaPath field) {
        return root.getValue(field.getRootSegment());
    }

    public void addField(SchemaPath field, DataValue value) {
        addField(field.getRootSegment(), value);
    }

    @Override
    public void addField(PathSegment segment, DataValue value) {
        root.addValue(segment, value);
    }

    @Override
    public void removeField(SchemaPath field) {
        root.removeValue(field.getRootSegment());
    }

    @Override
    public void write(DataWriter writer) throws IOException {
        writer.startRecord();
        root.write(writer);
        writer.endRecord();
    }

    public void merge(DataValue v) {
        if (v instanceof ContainerValue) {
            this.root = ValueUtils.getMergedDataValue(CollisionBehavior.MERGE_OVERRIDE, root, v);
        } else {
            this.root = v;
        }
    }

    public void merge(RecordPointer pointer) {
        if (pointer instanceof UnbackedRecord) {
            merge(UnbackedRecord.class.cast(pointer).root);
        } else {
            throw new UnsupportedOperationException(
                    String.format("Unable to merge from a record of type %s to an UnbackedRecord.", pointer.getClass().getCanonicalName())
            );
        }
    }

    @Override
    public RecordPointer copy() {
        UnbackedRecord r = new UnbackedRecord();
        r.root = this.root.copy();
        return r;
    }

    public void clear() {
        root = new SimpleMapValue();
    }

    public void setClearAndSetRoot(SchemaPath path, DataValue v) {
        root = new SimpleMapValue();
        root.addValue(path.getRootSegment(), v);
    }

    @Override
    public void copyFrom(RecordPointer r) {
        if (r instanceof UnbackedRecord) {
            this.root = ((UnbackedRecord) r).root.copy();
        } else {
            throw new UnsupportedOperationException(String.format("Unable to copy from a record of type %s to an UnbackedRecord.", r.getClass().getCanonicalName()));
        }
    }

    @Override
    public String toString() {
        return "UnbackedRecord [root=" + root + "]";
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((root == null) ? 0 : root.hashCode());
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) return true;
      if (obj == null) return false;
      if (getClass() != obj.getClass()) return false;
      UnbackedRecord other = (UnbackedRecord) obj;
      if (root == null) {
        if (other.root != null) return false;
      } else if (!root.equals(other.root)) return false;
      return true;
    }
    
    


}
