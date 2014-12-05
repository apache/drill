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
import org.apache.drill.exec.rpc.RemoteConnection;

import java.io.IOException;

/**
 * The Fragment Manager is responsible managing incoming data and executing a fragment. Once enough data and resources
 * are avialable, a fragment manager will start a fragment executor to run the associated fragment.
 */
public interface FragmentManager {

  /**
   * Handle the next incoming record batch.
   *
   * @param batch
   * @return True if the fragment has enough incoming data to be able to be run.
   * @throws FragmentSetupException, IOException
   */
  public abstract boolean handle(RawFragmentBatch batch) throws FragmentSetupException, IOException;

  /**
   * Get the fragment runner for this incoming fragment. Note, this can only be requested once.
   *
   * @return
   */
  public abstract FragmentExecutor getRunnable();

  public abstract void cancel();

  public boolean isWaiting();

  public abstract FragmentHandle getHandle();

  public abstract FragmentContext getFragmentContext();

  public abstract void addConnection(RemoteConnection connection);

  /**
   *  Sets autoRead property on all connections
   * @param autoRead
   */
  public abstract void setAutoRead(boolean autoRead);
}