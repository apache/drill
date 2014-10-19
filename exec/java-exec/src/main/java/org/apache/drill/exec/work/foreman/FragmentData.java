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
package org.apache.drill.exec.work.foreman;

import org.apache.drill.exec.proto.BitControl.FragmentStatus;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.proto.ExecProtos.FragmentHandle;
import org.apache.drill.exec.proto.UserBitShared.FragmentState;
import org.apache.drill.exec.proto.UserBitShared.MinorFragmentProfile;

public class FragmentData {
  private final boolean isLocal;
  private volatile FragmentStatus status;
  private volatile long lastStatusUpdate = 0;
  private final DrillbitEndpoint endpoint;

  public FragmentData(FragmentHandle handle, DrillbitEndpoint endpoint, boolean isLocal) {
    super();
    MinorFragmentProfile f = MinorFragmentProfile.newBuilder() //
        .setState(FragmentState.SENDING) //
        .setMinorFragmentId(handle.getMinorFragmentId()) //
        .setEndpoint(endpoint) //
        .build();
    this.status = FragmentStatus.newBuilder().setHandle(handle).setProfile(f).build();
    this.endpoint = endpoint;
    this.isLocal = isLocal;
  }

  public void setStatus(FragmentStatus status){
    this.status = status;
    lastStatusUpdate = System.currentTimeMillis();
  }

  public FragmentStatus getStatus() {
    return status;
  }

  public boolean isLocal() {
    return isLocal;
  }

  public long getLastStatusUpdate() {
    return lastStatusUpdate;
  }

  public DrillbitEndpoint getEndpoint() {
    return endpoint;
  }

  public FragmentHandle getHandle(){
    return status.getHandle();
  }

  @Override
  public String toString() {
    return "FragmentData [isLocal=" + isLocal + ", status=" + status + ", lastStatusUpdate=" + lastStatusUpdate
        + ", endpoint=" + endpoint + "]";
  }



}