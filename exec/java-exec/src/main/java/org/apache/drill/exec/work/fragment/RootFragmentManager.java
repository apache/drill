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

import org.apache.drill.exec.proto.BitControl.PlanFragment;
import org.apache.drill.exec.proto.ExecProtos.FragmentHandle;

/**
 * This managers determines when to run a root fragment node.
 */
public class RootFragmentManager extends AbstractFragmentManager {
  // private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(RootFragmentManager.class);

  public RootFragmentManager(final PlanFragment fragment, final FragmentExecutor fragmentExecutor,
                             final FragmentStatusReporter statusReporter) {
    super(fragment, fragmentExecutor, statusReporter);
  }

  @Override
  public void receivingFragmentFinished(final FragmentHandle handle) {
    throw new IllegalStateException("The root fragment should not be sending any messages to receiver.");
  }
}
