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
package org.apache.drill.exec.schema.daffodil;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.drill.common.util.JacksonUtils;
import org.apache.drill.common.AutoCloseables;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.coord.ClusterCoordinator;
import org.apache.drill.exec.coord.store.TransientStore;
import org.apache.drill.exec.coord.store.TransientStoreConfig;
import org.apache.drill.exec.exception.StoreException;
import org.apache.drill.exec.exception.VersionMismatchException;
import org.apache.drill.exec.proto.SchemaUserBitShared;
import org.apache.drill.exec.proto.UserBitShared.Registry;
import org.apache.drill.exec.store.sys.PersistentStoreConfig;
import org.apache.drill.exec.store.sys.PersistentStoreProvider;
import org.apache.drill.exec.store.sys.VersionedPersistentStore;
import org.apache.drill.exec.store.sys.store.DataChangeVersion;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static com.fasterxml.jackson.databind.SerializationFeature.INDENT_OUTPUT;

/**
 * Is responsible for remote Daffodil schema registry management.
 * Creates all remote registry areas at startup and validates them.
 * <p/>
 * Similar to RemoteFunctionRegistry but for Daffodil schemas.
 * <p/>
 * There are two schema stores:
 *
 * <li><b>REGISTRY</b> - persistent store, stores remote schema registry {@link Registry} under daffodil_schema path
 * which contains information about all dynamically registered schema jars.</li>
 *
 * <li><b>JARS</b> - transient store, stores information under daffodil_schema/jars path.
 * Serves as lock, not allowing to perform any action on the same jar at the same time.
 * There are two types of actions: {@link Action#REGISTRATION} and {@link Action#UNREGISTRATION}.</li>
 * <p/>
 * There are three schema areas:
 *
 * <li><b>STAGING</b> - area where user copies schema jars before starting registration process.</li>
 * <li><b>REGISTRY</b> - area where registered schema jars are stored.</li>
 * <li><b>TMP</b> - area where schema jars are backed up in unique folder during registration process.</li>
 */
public class RemoteDaffodilSchemaRegistry implements AutoCloseable {

  private static final String REGISTRY_PATH = "registry";
  private static final Logger logger = LoggerFactory.getLogger(RemoteDaffodilSchemaRegistry.class);
  private static final ObjectMapper mapper = JacksonUtils.createObjectMapper().enable(INDENT_OUTPUT);

  private int retryAttempts;
  private FileSystem fs;
  private Path registryArea;
  private Path stagingArea;
  private Path tmpArea;

  private VersionedPersistentStore<Registry> registry;
  private TransientStore<String> jars;

  public RemoteDaffodilSchemaRegistry() {
  }

  public void init(DrillConfig config, PersistentStoreProvider storeProvider, ClusterCoordinator coordinator) {
    prepareStores(storeProvider, coordinator);
    prepareAreas(config);
    this.retryAttempts = config.getInt(ExecConstants.UDF_RETRY_ATTEMPTS); // Reuse UDF retry attempts config
  }

  /**
   * Returns current remote schema registry version.
   * If remote schema registry is not found or unreachable, logs error and returns -1.
   *
   * @return remote schema registry version if any, -1 otherwise
   */
  public int getRegistryVersion() {
    DataChangeVersion version = new DataChangeVersion();
    boolean contains = false;
    try {
      contains = registry.contains(REGISTRY_PATH, version);
    } catch (Exception e) {
      logger.error("Problem during trying to access remote Daffodil schema registry [{}]", REGISTRY_PATH, e);
    }
    if (contains) {
      return version.getVersion();
    } else {
      logger.error("Remote Daffodil schema registry [{}] is unreachable", REGISTRY_PATH);
      return DataChangeVersion.NOT_AVAILABLE;
    }
  }

  /**
   * Report whether a remote registry exists.
   * @return true if a remote registry exists, false otherwise
   */
  public boolean hasRegistry() {
    return registry != null;
  }

  public Registry getRegistry(DataChangeVersion version) {
    return registry.get(REGISTRY_PATH, version);
  }

  public void updateRegistry(Registry registryContent, DataChangeVersion version) throws VersionMismatchException {
    registry.put(REGISTRY_PATH, registryContent, version);
  }

  public String addToJars(String jar, Action action) {
    return jars.putIfAbsent(jar, action.toString());
  }

  public void removeFromJars(String jar) {
    jars.remove(jar);
  }

  public int getRetryAttempts() {
    return retryAttempts;
  }

  public FileSystem getFs() {
    return fs;
  }

  public Path getRegistryArea() {
    return registryArea;
  }

  public Path getStagingArea() {
    return stagingArea;
  }

  public Path getTmpArea() {
    return tmpArea;
  }

  /**
   * Connects to two stores: REGISTRY and JARS.
   * Puts in REGISTRY store with default instance of remote schema registry if store is initiated for the first time.
   */
  private void prepareStores(PersistentStoreProvider storeProvider, ClusterCoordinator coordinator) {
    try {
      PersistentStoreConfig<Registry> registrationConfig = PersistentStoreConfig
          .newProtoBuilder(SchemaUserBitShared.Registry.WRITE, SchemaUserBitShared.Registry.MERGE)
          .name("daffodil_schema")
          .persist()
          .build();
      registry = storeProvider.getOrCreateVersionedStore(registrationConfig);
      logger.trace("Remote Daffodil schema registry type: {}.", registry.getClass());
      registry.putIfAbsent(REGISTRY_PATH, Registry.getDefaultInstance());
    } catch (StoreException e) {
      throw new DrillRuntimeException("Failure while loading remote Daffodil schema registry.", e);
    }

    TransientStoreConfig<String> jarsConfig = TransientStoreConfig.
        newJacksonBuilder(mapper, String.class).name("daffodil_schema/jars").build();
    jars = coordinator.getOrCreateTransientStore(jarsConfig);
  }

  /**
   * Creates if absent and validates three Daffodil schema areas: STAGING, REGISTRY and TMP.
   * Generated schema areas root from {@link ExecConstants#DFDL_DIRECTORY_ROOT},
   * if not set, uses user home directory instead.
   */
  private void prepareAreas(DrillConfig config) {
    logger.info("Preparing three remote Daffodil schema areas: staging, registry and tmp.");
    Configuration conf = new Configuration();
    if (config.hasPath(ExecConstants.DFDL_DIRECTORY_FS)) {
      conf.set(FileSystem.FS_DEFAULT_NAME_KEY, config.getString(ExecConstants.DFDL_DIRECTORY_FS));
    }

    try {
      this.fs = FileSystem.get(conf);
    } catch (IOException e) {
      throw DrillRuntimeException.create(e,
          "Error during file system %s setup", conf.get(FileSystem.FS_DEFAULT_NAME_KEY));
    }

    String root = fs.getHomeDirectory().toUri().getPath();
    if (config.hasPath(ExecConstants.DFDL_DIRECTORY_ROOT)) {
      root = config.getString(ExecConstants.DFDL_DIRECTORY_ROOT);
    }

    this.registryArea = createArea(fs, root, config.getString(ExecConstants.DFDL_DIRECTORY_REGISTRY));
    this.stagingArea = createArea(fs, root, config.getString(ExecConstants.DFDL_DIRECTORY_STAGING));
    this.tmpArea = createArea(fs, root, config.getString(ExecConstants.DFDL_DIRECTORY_TMP));
  }

  /**
   * Concatenates schema area with root directory.
   * Creates schema area, if area does not exist.
   * Checks if area exists and is directory, if it is writable for current user,
   * throws {@link org.apache.drill.common.exceptions.DrillRuntimeException} otherwise.
   *
   * @param fs file system where area should be created
   * @param root root directory
   * @param directory directory path
   * @return path to area
   */
  private Path createArea(FileSystem fs, String root, String directory) {
    Path path = new Path(root, directory);
    String filePath = path.toUri().getPath();
    try {
      if (!fs.exists(path)) {
        fs.mkdirs(path, new FsPermission(FsAction.ALL, FsAction.READ_EXECUTE, FsAction.NONE));
        logger.info("Created Daffodil schema directory at [{}].", filePath);
      }

      FileStatus fileStatus = fs.getFileStatus(path);
      if (!fileStatus.isDirectory()) {
        throw new DrillRuntimeException(String.format("Indicated path [%s] is not a directory.", filePath));
      }

      FsPermission permission = fileStatus.getPermission();
      FsAction userAction = permission.getUserAction();
      if (!userAction.implies(FsAction.READ_WRITE)) {
        throw new DrillRuntimeException(String.format("Unable to read or write into Daffodil schema directory [%s].", filePath));
      }

      logger.info("Daffodil schema directory [{}] has been validated.", filePath);
      return path;
    } catch (IOException e) {
      throw DrillRuntimeException.create(e, "Error during Daffodil schema area creation [%s].", filePath);
    }
  }

  @Override
  public void close() throws Exception {
    AutoCloseables.closeSilently(registry, jars);
  }

  /**
   * Enum for jar actions.
   */
  public enum Action {
    REGISTRATION,
    UNREGISTRATION
  }
}
