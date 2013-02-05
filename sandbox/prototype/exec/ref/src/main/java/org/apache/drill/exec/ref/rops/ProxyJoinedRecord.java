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

package org.apache.drill.exec.ref.rops;

import com.google.common.base.Objects;
import org.apache.drill.common.expression.PathSegment;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.ref.RecordPointer;
import org.apache.drill.exec.ref.values.DataValue;

import java.io.IOException;

public class ProxyJoinedRecord implements RecordPointer {
    RecordPointer left;
    RecordPointer right;

    public ProxyJoinedRecord() {
    }

    public void setRecord(RecordPointer left, RecordPointer right) {
        this.left = left;
        this.right = right;
    }

    @Override
    public DataValue getField(SchemaPath field) {
        DataValue leftVal = left != null ? left.getField(field) : null;
        if (leftVal != null) {
            return leftVal;
        }

        DataValue rightVal = right != null ? right.getField(field) : null;
        if (rightVal != null) {
            return rightVal;
        }

        return null;
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
    public void write(DataWriter writer) throws IOException {
        writer.startRecord();
        if (left != null) {
            left.write(writer);
        }

        if (right != null) {
            right.write(writer);
        }
        writer.endRecord();
    }

    @Override
    public RecordPointer copy() {
        ProxyJoinedRecord record = new ProxyJoinedRecord();
        record.setRecord(left != null ? left.copy() : null,
                right != null ? right.copy() : null);
        return record;
    }

    @Override
    public void copyFrom(RecordPointer r) {
        throw new UnsupportedOperationException();
    }
}
