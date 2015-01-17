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
package org.apache.drill.exec.physical.impl.mergereceiver;

import static org.apache.drill.exec.compile.sig.GeneratorMapping.GM;

import org.apache.drill.exec.compile.TemplateClassDefinition;
import org.apache.drill.exec.compile.sig.MappingSet;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.record.VectorAccessible;

public interface MergingReceiverGeneratorBase {

  public abstract void doSetup(FragmentContext context,
                               VectorAccessible incoming,
                               VectorAccessible outgoing) throws SchemaChangeException;

  public abstract int doEval(int leftIndex,
                                int rightIndex);

  public abstract void doCopy(int inIndex, int outIndex);

  public static TemplateClassDefinition<MergingReceiverGeneratorBase> TEMPLATE_DEFINITION =
      new TemplateClassDefinition<>(MergingReceiverGeneratorBase.class, MergingReceiverTemplate.class);

  public final MappingSet compareMapping =
    new MappingSet("leftIndex", "rightIndex",
      GM("doSetup", "doCompare", null, null),
      GM("doSetup", "doCompare", null, null));

  public final MappingSet copyMapping =
    new MappingSet("inIndex", "outIndex",
      GM("doSetup", "doCopy", null, null),
      GM("doSetup", "doCopy", null, null));

}
