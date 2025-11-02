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
import org.apache.drill.exec.exception.JarValidationException;
import org.apache.drill.exec.exception.VersionMismatchException;
import org.apache.drill.exec.physical.PhysicalPlan;
import org.apache.drill.exec.planner.sql.DirectPlan;
import org.apache.drill.exec.planner.sql.parser.SqlCreateDaffodilSchema;
import org.apache.drill.exec.proto.UserBitShared.Jar;
import org.apache.drill.exec.proto.UserBitShared.Registry;
import org.apache.drill.exec.schema.daffodil.RemoteDaffodilSchemaRegistry;
import org.apache.drill.exec.store.sys.store.DataChangeVersion;
import org.apache.drill.exec.work.foreman.ForemanSetupException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.UUID;

public class CreateDaffodilSchemaHandler extends DefaultSqlHandler {
  private static Logger logger = LoggerFactory.getLogger(CreateDaffodilSchemaHandler.class);

  public CreateDaffodilSchemaHandler(SqlHandlerConfig config) {
    super(config);
  }

  /**
   * Registers Daffodil schema JARs dynamically. Process consists of several steps:
   * <ol>
   * <li>Registering jar in jar registry to ensure that several jars with the same name is not registered.</li>
   * <li>Schema jar validation and back up.</li>
   * <li>Validation against remote schema registry.</li>
   * <li>Remote schema registry update.</li>
   * <li>Copying of jar to registry area and clean up.</li>
   * </ol>
   *
   * @return - Single row indicating successful registration, or error message otherwise.
   */
  @Override
  public PhysicalPlan getPlan(SqlNode sqlNode) throws ForemanSetupException, IOException {
    RemoteDaffodilSchemaRegistry remoteRegistry = context.getDaffodilSchemaRegistry();
    JarManager jarManager = new JarManager(sqlNode, remoteRegistry);

    boolean inProgress = false;
    try {
      final String action = remoteRegistry.addToJars(jarManager.getJarName(), RemoteDaffodilSchemaRegistry.Action.REGISTRATION);
      if (!(inProgress = action == null)) {
        return DirectPlan.createDirectPlan(context, false,
            String.format("Jar with %s name is used. Action: %s", jarManager.getJarName(), action));
      }

      jarManager.initRemoteBackup();
      initRemoteRegistration(jarManager, remoteRegistry);
      jarManager.deleteQuietlyFromStagingArea();

      return DirectPlan.createDirectPlan(context, true,
          String.format("Daffodil schema jar %s has been registered successfully.", jarManager.getJarName()));

    } catch (Exception e) {
      logger.error("Error during Daffodil schema registration", e);
      return DirectPlan.createDirectPlan(context, false, e.getMessage());
    } finally {
      if (inProgress) {
        remoteRegistry.removeFromJars(jarManager.getJarName());
      }
      jarManager.cleanUp();
    }
  }

  /**
   * Validates jar against remote jars to ensure no duplicate by jar name.
   *
   * @param remoteJars list of remote jars to validate against
   * @param jarName jar name to be validated
   * @throws JarValidationException in case of jar with the same name was found
   */
  private void validateAgainstRemoteRegistry(List<Jar> remoteJars, String jarName) {
    for (Jar remoteJar : remoteJars) {
      if (remoteJar.getName().equals(jarName)) {
        throw new JarValidationException(String.format("Jar with %s name has been already registered", jarName));
      }
    }
  }

  /**
   * Instantiates remote registration. First gets remote schema registry with version.
   * Version is used to ensure that we update the same registry we validated against.
   * Then validates against list of remote jars.
   * If validation is successful, first copies jar to registry area and starts updating remote schema registry.
   * If during update {@link VersionMismatchException} was detected,
   * attempts to repeat remote registration process till retry attempts exceeds the limit.
   * If retry attempts number hits 0, throws exception that failed to update remote schema registry.
   * In case of any error, if jar has been already copied to registry area, it will be deleted.
   *
   * @param jarManager helper class for copying jar to registry area
   * @param remoteRegistry remote schema registry
   * @throws IOException in case of problems with copying jar to registry area
   */
  private void initRemoteRegistration(JarManager jarManager,
      RemoteDaffodilSchemaRegistry remoteRegistry) throws IOException {
    int retryAttempts = remoteRegistry.getRetryAttempts();
    boolean copyJar = true;
    try {
      while (retryAttempts >= 0) {
        DataChangeVersion version = new DataChangeVersion();
        List<Jar> remoteJars = remoteRegistry.getRegistry(version).getJarList();
        validateAgainstRemoteRegistry(remoteJars, jarManager.getJarName());
        if (copyJar) {
          jarManager.copyToRegistryArea();
          copyJar = false;
        }
        List<Jar> jars = Lists.newArrayList(remoteJars);
        jars.add(Jar.newBuilder().setName(jarManager.getJarName()).build());
        Registry updatedRegistry = Registry.newBuilder().addAllJar(jars).build();
        try {
          remoteRegistry.updateRegistry(updatedRegistry, version);
          return;
        } catch (VersionMismatchException ex) {
          logger.debug("Failed to update schema registry during registration, version mismatch was detected.", ex);
          retryAttempts--;
        }
      }
      throw new DrillRuntimeException("Failed to update remote schema registry. Exceeded retry attempts limit.");
    } catch (Exception e) {
      if (!copyJar) {
        jarManager.deleteQuietlyFromRegistryArea();
      }
      throw e;
    }
  }

  /**
   * Inner helper class that encapsulates logic for working with schema jars.
   * During initialization it creates path to staging jar, remote temporary jar, and registry jar.
   * Is responsible for validation, copying and deletion actions.
   */
  private class JarManager {

    private final String jarName;
    private final FileSystem fs;

    private final Path remoteTmpDir;

    private final Path stagingJar;
    private final Path tmpRemoteJar;
    private final Path registryJar;

    JarManager(SqlNode sqlNode, RemoteDaffodilSchemaRegistry remoteRegistry) throws ForemanSetupException {
      SqlCreateDaffodilSchema node = unwrap(sqlNode, SqlCreateDaffodilSchema.class);
      this.jarName = ((SqlCharStringLiteral) node.getJar()).toValue();

      this.stagingJar = new Path(remoteRegistry.getStagingArea(), jarName);

      this.remoteTmpDir = new Path(remoteRegistry.getTmpArea(), UUID.randomUUID().toString());
      this.tmpRemoteJar = new Path(remoteTmpDir, jarName);

      this.registryJar = new Path(remoteRegistry.getRegistryArea(), jarName);

      this.fs = remoteRegistry.getFs();
    }

    /**
     * @return jar name
     */
    String getJarName() {
      return jarName;
    }

    /**
     * Validates that schema jar is present in staging area.
     * Backs up jar to unique folder in remote temporary area.
     *
     * @throws IOException in case of jar absence or problems during copying jar
     */
    void initRemoteBackup() throws IOException {
      checkPathExistence(stagingJar);
      fs.mkdirs(remoteTmpDir);
      FileUtil.copy(fs, stagingJar, fs, tmpRemoteJar, false, true, fs.getConf());
    }

    /**
     * Copies schema jar to registry area.
     *
     * @throws IOException is re-thrown in case of problems during copying process
     */
    void copyToRegistryArea() throws IOException {
      FileUtil.copy(fs, tmpRemoteJar, fs, registryJar, false, true, fs.getConf());
    }

    /**
     * Deletes schema jar from staging area, in case of problems, logs warning and proceeds.
     */
    void deleteQuietlyFromStagingArea() {
      deleteQuietly(stagingJar, false);
    }

    /**
     * Deletes schema jar from registry area, in case of problems, logs warning and proceeds.
     */
    void deleteQuietlyFromRegistryArea() {
      deleteQuietly(registryJar, false);
    }

    /**
     * Removes quietly remote temporary folder.
     */
    void cleanUp() {
      deleteQuietly(remoteTmpDir, true);
    }
    /**
     * Checks if passed path exists on predefined file system.
     *
     * @param path path to be checked
     * @throws IOException if path does not exist
     */
    private void checkPathExistence(Path path) throws IOException {
      if (!fs.exists(path)) {
        throw new IOException(String.format("File %s does not exist on file system %s",
            path.toUri().getPath(), fs.getUri()));
      }
    }

    /**
     * Deletes quietly file or directory, in case of errors, logs warning and proceeds.
     *
     * @param path path to file or directory
     * @param isDirectory set to true if we need to delete a directory
     */
    private void deleteQuietly(Path path, boolean isDirectory) {
      try {
        fs.delete(path, isDirectory);
      } catch (IOException e) {
        logger.warn(String.format("Error during deletion [%s]", path.toUri().getPath()), e);
      }
    }
  }
}
