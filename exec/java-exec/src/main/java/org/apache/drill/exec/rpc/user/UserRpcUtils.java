/*
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
package org.apache.drill.exec.rpc.user;

import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;

import org.apache.drill.common.util.DrillVersionInfo;
import org.apache.drill.exec.proto.UserProtos.RpcEndpointInfos;

import com.google.common.base.Preconditions;

/**
 * Utility class for User RPC
 *
 */
public final class UserRpcUtils {
  private UserRpcUtils() {}

  /**
   * Returns a {@code RpcEndpointInfos} instance
   *
   * The instance is populated based on Drill version informations
   * from the classpath and runtime information for the application
   * name.
   *
   * @param name the endpoint name.
   * @return a {@code RpcEndpointInfos} instance
   * @throws NullPointerException if name is null
   */
  public static RpcEndpointInfos getRpcEndpointInfos(String name) {
    RuntimeMXBean mxBean = ManagementFactory.getRuntimeMXBean();
    RpcEndpointInfos infos = RpcEndpointInfos.newBuilder()
        .setName(Preconditions.checkNotNull(name))
        .setApplication(mxBean.getName())
        .setVersion(DrillVersionInfo.getVersion())
        .setMajorVersion(DrillVersionInfo.getMajorVersion())
        .setMinorVersion(DrillVersionInfo.getMinorVersion())
        .setPatchVersion(DrillVersionInfo.getPatchVersion())
        .build();

    return infos;
  }
}
