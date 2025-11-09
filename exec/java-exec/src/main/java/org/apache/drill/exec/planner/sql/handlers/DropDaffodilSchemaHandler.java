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
package org.apache.drill.exec.planner.sql.handlers;

import com.google.common.collect.Lists;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.exec.exception.VersionMismatchException;
import org.apache.drill.exec.physical.PhysicalPlan;
import org.apache.drill.exec.planner.sql.DirectPlan;
import org.apache.drill.exec.planner.sql.parser.SqlDropDaffodilSchema;
import org.apache.drill.exec.proto.UserBitShared.Jar;
import org.apache.drill.exec.proto.UserBitShared.Registry;
import org.apache.drill.exec.schema.daffodil.RemoteDaffodilSchemaRegistry;
import org.apache.drill.exec.store.sys.store.DataChangeVersion;
import org.apache.drill.exec.work.foreman.ForemanSetupException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

public class DropDaffodilSchemaHandler extends DefaultSqlHandler {
  private static Logger logger = LoggerFactory.getLogger(DropDaffodilSchemaHandler.class);

  public DropDaffodilSchemaHandler(SqlHandlerConfig config) {
    super(config);
  }

  /**
   * Unregisters Daffodil schema JARs dynamically. Process consists of several steps:
   * <ol>
   * <li>Registering jar in jar registry to ensure that the jar is not being unregistered elsewhere.</li>
   * <li>Starts remote unregistration process, gets list of all jars and excludes jar to be deleted.</li>
   * <li>Removes jar from registry area.</li>
   * </ol>
   *
   * Only jars registered dynamically can be unregistered.
   *
   * Limitation: before jar unregistration make sure no one is using schemas from this jar.
   * There is no guarantee that running queries will finish successfully or give correct result.
   *
   * @return - Single row indicating successful unregistration, raise exception otherwise
   */
  @Override
  public PhysicalPlan getPlan(SqlNode sqlNode) throws ForemanSetupException, IOException {
    SqlDropDaffodilSchema node = unwrap(sqlNode, SqlDropDaffodilSchema.class);
    String jarName = ((SqlCharStringLiteral) node.getJar()).toValue();
    RemoteDaffodilSchemaRegistry remoteSchemaRegistry = context.getDaffodilSchemaRegistry();

    boolean inProgress = false;
    try {
      final String action = remoteSchemaRegistry.addToJars(jarName, RemoteDaffodilSchemaRegistry.Action.UNREGISTRATION);
      if (!(inProgress = action == null)) {
        return DirectPlan.createDirectPlan(context, false, String.format("Jar with %s name is used. Action: %s", jarName, action));
      }

      Jar deletedJar = unregister(jarName, remoteSchemaRegistry);
      if (deletedJar == null) {
        return DirectPlan.createDirectPlan(context, false, String.format("Jar %s is not registered in remote registry", jarName));
      }

      removeJarFromArea(jarName, remoteSchemaRegistry.getFs(), remoteSchemaRegistry.getRegistryArea());

      return DirectPlan.createDirectPlan(context, true,
          String.format("Daffodil schema jar %s has been unregistered successfully.", jarName));

    } catch (Exception e) {
      logger.error("Error during Daffodil schema unregistration", e);
      return DirectPlan.createDirectPlan(context, false, e.getMessage());
    } finally {
      if (inProgress) {
        remoteSchemaRegistry.removeFromJars(jarName);
      }
    }
  }

  /**
   * Gets remote schema registry with version.
   * Version is used to ensure that we update the same registry we removed jars from.
   * Looks for a jar to be deleted, if found,
   * attempts to update remote registry with list of jars, that excludes jar to be deleted.
   * If during update {@link VersionMismatchException} was detected,
   * attempts to repeat unregistration process till retry attempts exceeds the limit.
   * If retry attempts number hits 0, throws exception that failed to update remote schema registry.
   *
   * @param jarName jar name
   * @param remoteSchemaRegistry remote schema registry
   * @return jar that was unregistered, null otherwise
   */
  private Jar unregister(String jarName, RemoteDaffodilSchemaRegistry remoteSchemaRegistry) {
    int retryAttempts = remoteSchemaRegistry.getRetryAttempts();
    while (retryAttempts >= 0) {
      DataChangeVersion version = new DataChangeVersion();
      Registry registry = remoteSchemaRegistry.getRegistry(version);
      Jar jarToBeDeleted = null;
      List<Jar> jars = Lists.newArrayList();
      for (Jar j : registry.getJarList()) {
        if (j.getName().equals(jarName)) {
          jarToBeDeleted = j;
        } else {
          jars.add(j);
        }
      }
      if (jarToBeDeleted == null) {
        return null;
      }
      Registry updatedRegistry = Registry.newBuilder().addAllJar(jars).build();
      try {
        remoteSchemaRegistry.updateRegistry(updatedRegistry, version);
        return jarToBeDeleted;
      } catch (VersionMismatchException ex) {
        logger.debug("Failed to update schema registry during unregistration, version mismatch was detected.", ex);
        retryAttempts--;
      }
    }
    throw new DrillRuntimeException("Failed to update remote schema registry. Exceeded retry attempts limit.");
  }

  /**
   * Removes jar from indicated area, in case of error log it and proceeds.
   *
   * @param jarName jar name
   * @param fs file system
   * @param area path to area
   */
  private void removeJarFromArea(String jarName, FileSystem fs, Path area) {
    try {
      fs.delete(new Path(area, jarName), false);
    } catch (IOException e) {
      logger.error("Error removing jar {} from area {}", jarName, area.toUri().getPath());
    }
  }
}
