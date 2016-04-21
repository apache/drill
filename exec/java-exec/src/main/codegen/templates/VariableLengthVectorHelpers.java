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

import java.lang.Override;

import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.drill.exec.vector.BaseDataValueVector;
import org.apache.drill.exec.vector.BaseValueVector;
import org.apache.drill.exec.vector.VariableWidthVector;

<@pp.dropOutputFile />
<#list vv.types as type>
<#list type.minor as minor>

<#assign friendlyType = (minor.friendlyType!minor.boxedType!type.boxedType) />

<#if type.major == "VarLen">
<@pp.changeOutputFile name="/org/apache/arrow/vector/${minor.class}VectorHelper.java" />

<#include "/@includes/license.ftl" />

package org.apache.arrow.vector;

<#include "/@includes/vv_imports.ftl" />

public final class ${minor.class}VectorHelper extends BaseValueVectorHelper {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(${minor.class}Vector.class);

  private ${minor.class}Vector vector;

  public ${minor.class}VectorHelper(${minor.class}Vector vector) {
    super(vector);
    this.vector = vector;
  }

  public SerializedField getMetadata() {
    return getMetadataBuilder() //
             .addChild(TypeHelper.getMetadata(vector.offsetVector))
             .setValueCount(vector.getAccessor().getValueCount()) //
             .setBufferLength(vector.getBufferSize()) //
             .build();
  }

  public void load(SerializedField metadata, ArrowBuf buffer) {
//     the bits vector is the first child (the order in which the children are added in getMetadataBuilder is significant)
    final SerializedField offsetField = metadata.getChild(0);
    TypeHelper.load(vector.offsetVector, offsetField, buffer);

    final int capacity = buffer.capacity();
    final int offsetsLength = offsetField.getBufferLength();
    vector.data = buffer.slice(offsetsLength, capacity - offsetsLength);
    vector.data.retain();
  }
}
</#if> <#-- type.major -->
</#list>
</#list>
