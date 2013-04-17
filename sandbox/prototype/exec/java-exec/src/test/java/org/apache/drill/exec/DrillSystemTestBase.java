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
package org.apache.drill.exec;

import com.google.common.collect.ImmutableList;
import org.apache.drill.exec.exception.DrillbitStartupException;
import org.apache.drill.exec.server.Drillbit;
import org.apache.drill.exec.server.StartupOptions;

import java.util.List;

import static com.google.common.base.Throwables.propagate;

/**
 * Base class for Drill system tests.
 * Starts one or more Drillbits and provides a configured client for testing.
 */
public class DrillSystemTestBase {

  private static List<Drillbit> servers;

  public void startCluster(StartupOptions options, int numServers) {
    try {
      ImmutableList.Builder<Drillbit> servers = ImmutableList.builder();
      for (int i = 0; i < numServers; i++) {
        servers.add(Drillbit.start(options));
      }
      this.servers = servers.build();
    } catch (DrillbitStartupException e) {
      propagate(e);
    }
  }

  public void startZookeeper() {

  }

  public void stopCluster() {

  }

  public void stopZookeeper() {

  }

}
