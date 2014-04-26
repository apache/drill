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
package org.apache.drill.exec.schema;

import org.apache.drill.common.types.TypeProtos.MajorType;

import com.google.common.base.Objects;

public class OrderedField extends Field {
    private final int index;

    public OrderedField(RecordSchema parentSchema,
                        MajorType type,
                        String prefixFieldName,
                        int index) {
        super(parentSchema, type, prefixFieldName);
        this.index = index;
    }

    @Override
    public String getFieldName() {
        return "[" + index + "]";
    }

    @Override
    protected Objects.ToStringHelper addAttributesToHelper(Objects.ToStringHelper helper) {
        return helper;
    }
}
