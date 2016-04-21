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
import org.apache.drill.exec.vector.BaseDataValueVector;
import org.apache.drill.exec.vector.NullableVectorDefinitionSetter;

import java.lang.Override;
import java.lang.UnsupportedOperationException;

<@pp.dropOutputFile />
<#list vv.types as type>
<#list type.minor as minor>

<#assign className = "Nullable${minor.class}VectorHelper" />
<#assign valuesName = "${minor.class}Vector" />
<#assign friendlyType = (minor.friendlyType!minor.boxedType!type.boxedType) />

<@pp.changeOutputFile name="/org/apache/arrow/vector/${className}.java" />

<#include "/@includes/license.ftl" />

package org.apache.arrow.vector;

<#include "/@includes/vv_imports.ftl" />

/**
 * Nullable${minor.class} implements a vector of values which could be null.  Elements in the vector
 * are first checked against a fixed length vector of boolean values.  Then the element is retrieved
 * from the base class (if not null).
 *
 * NB: this class is automatically generated from ${.template_name} and ValueVectorTypes.tdd using FreeMarker.
 */
@SuppressWarnings("unused")
public final class ${className} extends BaseValueVectorHelper {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(${className}.class);

  private Nullable${minor.class}Vector vector;

  public ${className} (Nullable${minor.class}Vector vector) {
    super(vector);
    this.vector = vector;
  }

  public SerializedField.Builder getMetadataBuilder() {
    return super.getMetadataBuilder()
      .addChild(TypeHelper.getMetadata(vector.bits))
      .addChild(TypeHelper.getMetadata(vector.values));
  }

  public void load(SerializedField metadata, ArrowBuf buffer) {
    vector.clear();
//         the bits vector is the first child (the order in which the children are added in getMetadataBuilder is significant)
    final SerializedField bitsField = metadata.getChild(0);
    TypeHelper.load(vector.bits, bitsField, buffer);

    final int capacity = buffer.capacity();
    final int bitsLength = bitsField.getBufferLength();
    final SerializedField valuesField = metadata.getChild(1);
    TypeHelper.load(vector.values, valuesField, buffer.slice(bitsLength, capacity - bitsLength));
  }

}
</#list>
</#list>
