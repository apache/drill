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
package org.apache.drill.exec.pop.receiver;

import java.util.List;

import org.apache.drill.common.physical.pop.base.AbstractReceiver;
import org.apache.drill.common.physical.pop.base.PhysicalVisitor;
import org.apache.drill.common.proto.CoordinationProtos.DrillbitEndpoint;

import com.fasterxml.jackson.annotation.JsonTypeName;

@JsonTypeName("nway-ordering-receiver")
public class NWayOrderingReceiver extends AbstractReceiver{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(NWayOrderingReceiver.class);

  @Override
  public List<DrillbitEndpoint> getProvidingEndpoints() {
    return null;
  }

  @Override
  public boolean supportsOutOfOrderExchange() {
    return false;
  }

  @Override
  public int getSenderCount() {
    return 0;
  }

  @Override
  public <T, X, E extends Throwable> T accept(PhysicalVisitor<T, X, E> physicalVisitor, X value) throws E {
    return null;
  }

}
