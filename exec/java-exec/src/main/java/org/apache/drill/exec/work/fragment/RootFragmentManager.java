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

import org.apache.drill.exec.exception.FragmentSetupException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.proto.ExecProtos.FragmentHandle;
import org.apache.drill.exec.record.RawFragmentBatch;
import org.apache.drill.exec.work.batch.IncomingBuffers;

public class RootFragmentManager implements FragmentManager{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(RootFragmentManager.class);

  private final IncomingBuffers buffers;
  private final FragmentExecutor runner;
  private final FragmentHandle handle;
  private volatile boolean cancel = false;
  
  public RootFragmentManager(FragmentHandle handle, IncomingBuffers buffers, FragmentExecutor runner) {
    super();
    this.handle = handle;
    this.buffers = buffers;
    this.runner = runner;
  }

  @Override
  public boolean handle(RawFragmentBatch batch) throws FragmentSetupException {
    return buffers.batchArrived(batch);
  }

  @Override
  public FragmentExecutor getRunnable() {
    return runner;
  }

  public FragmentHandle getHandle() {
    return handle;
  }

  @Override
  public void cancel() {
    cancel = true;
  }

  @Override
  public boolean isWaiting() {
    return !buffers.isDone() && !cancel;
  }

  @Override
  public FragmentContext getFragmentContext() {
    return runner.getContext();
  }
  
  
  
}
