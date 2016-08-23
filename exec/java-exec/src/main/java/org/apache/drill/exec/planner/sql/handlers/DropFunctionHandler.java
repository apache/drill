/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.planner.sql.handlers;

import com.google.common.collect.Lists;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.exception.VersionMismatchException;
import org.apache.drill.exec.expr.fn.RemoteFunctionRegistry;
import org.apache.drill.exec.physical.PhysicalPlan;
import org.apache.drill.exec.planner.sql.DirectPlan;
import org.apache.drill.exec.planner.sql.parser.SqlDropFunction;
import org.apache.drill.exec.proto.UserBitShared.Jar;
import org.apache.drill.exec.proto.UserBitShared.Registry;
import org.apache.drill.exec.store.sys.store.DataChangeVersion;
import org.apache.drill.exec.util.JarUtil;
import org.apache.drill.exec.work.foreman.ForemanSetupException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.List;

public class DropFunctionHandler extends DefaultSqlHandler {

  private static org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DropFunctionHandler.class);

  public DropFunctionHandler(SqlHandlerConfig config) {
    super(config);
  }

  /**
   * Drops UDFs dynamically.
   * @return - Single row indicating list of unregistered UDFs, raise exception otherwise
   */
  @Override
  public PhysicalPlan getPlan(SqlNode sqlNode) throws ForemanSetupException, IOException {
    if (context.getOption(ExecConstants.DYNAMIC_UDF_SUPPORT_ENABLED).bool_val) {

      SqlDropFunction node = unwrap(sqlNode, SqlDropFunction.class);
      String jarName = ((SqlCharStringLiteral) node.getJar()).toValue();
      RemoteFunctionRegistry remoteFunctionRegistry = context.getRemoteFunctionRegistry();

      boolean inProgress = false;
      try {
        final String action = remoteFunctionRegistry.addToJars(jarName, RemoteFunctionRegistry.Action.UNREGISTRATION);
        if (!(inProgress = action == null)) {
          return DirectPlan.createDirectPlan(context, false,
              String.format("Jar with %s name is used. Action: %s", jarName, action));
        }

        remoteFunctionRegistry.submitForUnregistration(jarName);

        Jar deletedJar = unregister(jarName, remoteFunctionRegistry, remoteFunctionRegistry.getRetryTimes());
        if (deletedJar == null) {
          return DirectPlan.createDirectPlan(context, false,
              String.format("Jar %s is not registered in remote registry", jarName));
        }

        FileSystem fs = remoteFunctionRegistry.getFs();
        fs.delete(new Path(remoteFunctionRegistry.getRegistryArea(), jarName), false);
        String sourceName = JarUtil.getSourceName(jarName);
        fs.delete(new Path(remoteFunctionRegistry.getRegistryArea(), sourceName), false);

        return DirectPlan.createDirectPlan(context, true,
            String.format("The following UDFs in jar %s have been unregistered:\n%s",
                jarName, deletedJar.getFunctionList()));
      } finally {
        if (inProgress) {
          remoteFunctionRegistry.finishUnregistration(jarName);
          remoteFunctionRegistry.removeFromJars(jarName);
        }
      }
    }
    throw UserException.validationError()
        .message("Dynamic UDFs support is disabled.")
        .build(logger);
  }

  private Jar unregister(String jarName, RemoteFunctionRegistry remoteFunctionRegistry, int retryTimes) {
    DataChangeVersion version = new DataChangeVersion();
    Registry registry = remoteFunctionRegistry.getRegistry(version);
    Jar jarToBeDeleted = null;
    List<Jar> jars = Lists.newArrayList();
    for (Jar j : registry.getJarList()) {
      if (j.getName().equals(jarName)) {
        jarToBeDeleted = j;
      } else {
        jars.add(j);
      }
    }
    if (jarToBeDeleted != null) {
      Registry updatedRegistry = Registry.newBuilder().addAllJar(jars).build();
      try {
        remoteFunctionRegistry.updateRegistry(updatedRegistry, version);
      } catch (VersionMismatchException ex) {
        if (retryTimes-- == 0) {
          throw new DrillRuntimeException("Failed to update remote function registry. Exceeded retry times limit.");
        }
        unregister(jarName, remoteFunctionRegistry, retryTimes);
      }
    }
    return jarToBeDeleted;
  }
}