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
package org.apache.drill.exec.physical;

import org.apache.drill.exec.physical.base.Size;

public class ReadEntryWithPath implements ReadEntry {

  protected String path;

  
  public ReadEntryWithPath(String path) {
    super();
    this.path = path;
  }

  public ReadEntryWithPath(){}
  
  public String getPath(){
   return path;
  }

  @Override
  public OperatorCost getCost() {
    throw new UnsupportedOperationException(this.getClass().getCanonicalName() + " is only for extracting path data from " +
        "selections inside a scan node from a logical plan, it cannot be used in an executing plan and has no cost.");
  }

  @Override
  public Size getSize() {
    throw new UnsupportedOperationException(this.getClass().getCanonicalName() + " is only for extracting path data from " +
        "selections on a scan node from a logical plan, it cannot be used in an executing plan and has no size.");
  }
}
