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

import org.apache.arrow.vector.util.TransferPair;
import org.apache.drill.exec.vector.complex.BaseRepeatedValueVector;
import org.mortbay.jetty.servlet.Holder;

<@pp.dropOutputFile />
<#list vv.types as type>
<#list type.minor as minor>
<#assign friendlyType = (minor.friendlyType!minor.boxedType!type.boxedType) />
<#assign fields = minor.fields!type.fields />

<@pp.changeOutputFile name="/org/apache/arrow/vector/Repeated${minor.class}VectorHelper.java" />
<#include "/@includes/license.ftl" />

package org.apache.arrow.vector;

<#include "/@includes/vv_imports.ftl" />

public final class Repeated${minor.class}VectorHelper extends BaseRepeatedValueVectorHelper {

  private Repeated${minor.class}Vector vector;

  public Repeated${minor.class}VectorHelper(Repeated${minor.class}Vector vector) {
        super(vector);
        this.vector = vector;
  }

  <#if type.major == "VarLen">
  public SerializedField.Builder getMetadataBuilder() {
    return super.getMetadataBuilder()
            .setVarByteLength(vector.values.getVarByteLength());
  }
  </#if>
}
</#list>
</#list>
