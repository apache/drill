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
package org.apache.drill.exec.work;

import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.proto.ExecProtos.FragmentHandle;
import org.apache.drill.exec.proto.ExecProtos.FragmentStatus;
import org.apache.drill.exec.proto.ExecProtos.FragmentStatus.Builder;
import org.apache.drill.exec.proto.ExecProtos.FragmentStatus.FragmentState;
import org.apache.drill.exec.rpc.bit.BitTunnel;
import org.apache.drill.exec.work.foreman.ErrorHelper;

/**
 * Informs remote node as fragment changes state.
 */
public class RemotingFragmentRunnerListener extends AbstractFragmentRunnerListener{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(RemotingFragmentRunnerListener.class);
  
  private final BitTunnel tunnel;

  public RemotingFragmentRunnerListener(FragmentContext context, BitTunnel tunnel) {
    super(context);
    this.tunnel = tunnel;
  }
  
  
  @Override
  protected void statusChange(FragmentHandle handle, FragmentStatus status) {
    logger.debug("Sending status change message message to remote node: " + status);
    tunnel.sendFragmentStatus(status);
  }
  
}
