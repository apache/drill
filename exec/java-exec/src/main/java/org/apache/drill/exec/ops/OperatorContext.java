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
package org.apache.drill.exec.ops;

import com.google.common.util.concurrent.ListenableFuture;
import io.netty.buffer.DrillBuf;

import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.Callable;

import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.testing.ExecutionControls;
import org.apache.drill.exec.store.dfs.DrillFileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;

public abstract class OperatorContext {

  public abstract DrillBuf replace(DrillBuf old, int newSize);

  public abstract DrillBuf getManagedBuffer();

  public abstract DrillBuf getManagedBuffer(int size);

  public abstract BufferAllocator getAllocator();

  public abstract OperatorStats getStats();

  public abstract ExecutionControls getExecutionControls();

  public abstract DrillFileSystem newFileSystem(Configuration conf) throws IOException;

  /**
   * Run the callable as the given proxy user.
   *
   * @param proxyUgi proxy user group information
   * @param callable callable to run
   * @param <RESULT> result type
   * @return Future<RESULT> future with the result of calling the callable
   */
  public abstract <RESULT> ListenableFuture<RESULT> runCallableAs(UserGroupInformation proxyUgi,
                                                                  Callable<RESULT> callable);

  public static int getChildCount(PhysicalOperator popConfig) {
    Iterator<PhysicalOperator> iter = popConfig.iterator();
    int i = 0;
    while (iter.hasNext()) {
      iter.next();
      i++;
    }

    if (i == 0) {
      i = 1;
    }
    return i;
  }

}