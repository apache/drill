/*
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
package org.apache.drill.exec.physical.impl.orderedpartitioner;

import javax.inject.Named;

import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.record.VectorAccessible;
import org.apache.drill.exec.record.selection.SelectionVector4;

public abstract class SampleCopierTemplate implements SampleCopier {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SampleCopierTemplate.class);

  private SelectionVector4 sv4;
  private int outputRecords = 0;

  @Override
  public void setupCopier(FragmentContext context, SelectionVector4 sv4, VectorAccessible incoming, VectorAccessible outgoing)
          throws SchemaChangeException{
    this.sv4 = sv4;
    doSetup(context, incoming, outgoing);
  }

  @Override
  public int getOutputRecords() {
    return outputRecords;
  }


  @Override
  public boolean copyRecords(int skip, int start, int total) {
    final int recordCount = sv4.getCount();
    int outgoingPosition = 0;
    int increment = skip > 0 ? skip : 1;
    for(int svIndex = start; svIndex < sv4.getCount() && outputRecords < total; svIndex += increment, outgoingPosition++){
      int deRefIndex = sv4.get(svIndex);
      if (!doEval(deRefIndex, outgoingPosition)) {
        return false;
      }
      outputRecords++;
    }
    return true;
  }

  public abstract void doSetup(@Named("context") FragmentContext context, @Named("incoming") VectorAccessible incoming, @Named("outgoing") VectorAccessible outgoing);
  public abstract boolean doEval(@Named("inIndex") int inIndex, @Named("outIndex") int outIndex);



}
