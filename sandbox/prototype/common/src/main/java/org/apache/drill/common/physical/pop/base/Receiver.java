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
package org.apache.drill.common.physical.pop.base;

import java.util.List;

import org.apache.drill.common.proto.CoordinationProtos.DrillbitEndpoint;

public interface Receiver extends FragmentLeaf {
  public abstract List<DrillbitEndpoint> getProvidingEndpoints();

  /**
   * Whether or not this receive supports out of order exchange. This provides a hint for the scheduling node on whether
   * the receiver can start work if only a subset of all sending endpoints are currently providing data. A random
   * receiver would supports this form of operation. A NWAY receiver would not.
   * 
   * @return True if this receiver supports working on a streaming/out of order input.
   */
  public abstract boolean supportsOutOfOrderExchange();
  
  
  public int getSenderCount();
}
