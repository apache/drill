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

<@pp.dropOutputFile />
<@pp.changeOutputFile name="org/apache/drill/exec/store/AbstractRecordWriter.java" />
<#include "/@includes/license.ftl" />

package org.apache.drill.exec.store;

import org.apache.drill.exec.expr.holders.*;

import java.io.IOException;
import java.lang.UnsupportedOperationException;

public abstract class AbstractRecordWriter implements RecordWriter {

<#list vv.types as type>
  <#list type.minor as minor>
    <#list vv.modes as mode>
  @Override
  public void add${mode.prefix}${minor.class}Holder(int fieldId, ${mode.prefix}${minor.class}Holder valueHolder) throws IOException {
    throw new UnsupportedOperationException("Doesn't support writing '${mode.prefix}${minor.class}Holder'");
  }
    </#list>
  </#list>
</#list>
}
