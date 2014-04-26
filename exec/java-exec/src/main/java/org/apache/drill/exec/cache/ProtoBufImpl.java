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
package org.apache.drill.exec.cache;

import org.apache.drill.exec.proto.BitControl.PlanFragment;
import org.apache.drill.exec.proto.BitControl.WorkQueueStatus;
import org.apache.drill.exec.proto.ExecProtos.FragmentHandle;

import com.hazelcast.core.HazelcastInstance;

public class ProtoBufImpl {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ProtoBufImpl.class);
  
  public static class HWorkQueueStatus extends ProtoBufWrap<WorkQueueStatus>{
    public HWorkQueueStatus() {super(WorkQueueStatus.PARSER);}
    public HWorkQueueStatus(WorkQueueStatus value) {super(value, WorkQueueStatus.PARSER);}
  }
  
  public static class HFragmentHandle extends ProtoBufWrap<FragmentHandle>{
    public HFragmentHandle() {super(FragmentHandle.PARSER);}
    public HFragmentHandle(FragmentHandle value) {super(value, FragmentHandle.PARSER);}
  }
  
  public static class HPlanFragment extends ProtoBufWrap<PlanFragment>{
    public HPlanFragment() {super(PlanFragment.PARSER);}
    public HPlanFragment(PlanFragment value) {super(value, PlanFragment.PARSER);}
  }
  
  public static class HandlePlan extends ProtoMap<FragmentHandle, PlanFragment, HFragmentHandle, HPlanFragment>{
    public HandlePlan(HazelcastInstance instance) {super(instance, "plan-fragment-cache");}
    public HFragmentHandle getNewKey(FragmentHandle key) {return new HFragmentHandle(key);}
    public HPlanFragment getNewValue(PlanFragment value) {return new HPlanFragment(value);}
  }
}
