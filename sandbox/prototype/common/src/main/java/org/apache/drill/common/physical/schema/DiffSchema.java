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

package org.apache.drill.common.physical.schema;

import com.google.common.collect.Lists;

import java.util.List;

public class DiffSchema {
    List<Field> addedFields;
    List<Field> removedFields;

    public DiffSchema() {
        this.addedFields = Lists.newArrayList();
        this.removedFields = Lists.newArrayList();
    }

    public void recordNewField(Field field) {
        addedFields.add(field);
    }

    public boolean hasDiffFields() {
        return !addedFields.isEmpty() || !removedFields.isEmpty();
    }

    public List<Field> getAddedFields() {
        return addedFields;
    }

    public List<Field> getRemovedFields() {
        return removedFields;
    }

    public void reset() {
        addedFields.clear();
        removedFields.clear();
    }

    public void addRemovedField(Field field) {
        removedFields.add(field);
    }

    @Override
    public String toString() {
        return "DiffSchema{" +
                "addedFields=" + addedFields +
                ", removedFields=" + removedFields +
                '}';
    }
}
