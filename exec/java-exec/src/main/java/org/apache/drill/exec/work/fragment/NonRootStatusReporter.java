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
package org.apache.drill.exec.work.fragment;

import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.proto.BitControl.FragmentStatus;
import org.apache.drill.exec.proto.ExecProtos.FragmentHandle;
import org.apache.drill.exec.rpc.control.ControlTunnel;

/**
 * For all non root fragments, status will be reported back to the foreman through a control tunnel.
 */
public class NonRootStatusReporter extends AbstractStatusReporter{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(NonRootStatusReporter.class);

  private final ControlTunnel tunnel;

  public NonRootStatusReporter(FragmentContext context, ControlTunnel tunnel) {
    super(context);
    this.tunnel = tunnel;
  }

  @Override
  protected void statusChange(FragmentHandle handle, FragmentStatus status) {
    logger.debug("Sending status change message message to remote node: " + status);
    tunnel.sendFragmentStatus(status);
  }

}
