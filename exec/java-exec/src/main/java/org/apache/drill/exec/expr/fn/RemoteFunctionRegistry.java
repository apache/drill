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
package org.apache.drill.exec.expr.fn;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.FilenameUtils;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.coord.ClusterCoordinator;
import org.apache.drill.exec.coord.store.TransientStore;
import org.apache.drill.exec.coord.store.TransientStoreConfig;
import org.apache.drill.exec.coord.store.TransientStoreListener;
import org.apache.drill.exec.exception.StoreException;
import org.apache.drill.exec.exception.VersionMismatchException;
import org.apache.drill.exec.proto.SchemaUserBitShared;
import org.apache.drill.exec.proto.UserBitShared.Registry;
import org.apache.drill.exec.store.sys.PersistentStore;
import org.apache.drill.exec.store.sys.PersistentStoreConfig;
import org.apache.drill.exec.store.sys.PersistentStoreProvider;
import org.apache.drill.exec.store.sys.store.DataChangeVersion;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

import static com.fasterxml.jackson.databind.SerializationFeature.INDENT_OUTPUT;

/** Is responsible for remote function registry management.
 *  Creates all remote registry areas at startup, during init establishes connections with stores.
 *  Provides tools to update remote registry and unregister functions, access remote registry areas.
 */
public class RemoteFunctionRegistry implements AutoCloseable {

  public static final String REGISTRY = "registry";

  private static final ObjectMapper mapper = new ObjectMapper().enable(INDENT_OUTPUT);

  private final TransientStoreListener unregistrationListener;
  private int retryTimes;
  private FileSystem fs;
  private Path registryArea;
  private Path stagingArea;
  private Path tmpArea;

  private PersistentStore<Registry> registry;
  private TransientStore<String> unregistration;
  private TransientStore<String> jars;

  public RemoteFunctionRegistry(TransientStoreListener unregistrationListener) {
    this.unregistrationListener = unregistrationListener;
  }

  public void init(DrillConfig config, PersistentStoreProvider storeProvider, ClusterCoordinator coordinator) {
    try {
      PersistentStoreConfig<Registry> registrationConfig = PersistentStoreConfig
          .newProtoBuilder(SchemaUserBitShared.Registry.WRITE, SchemaUserBitShared.Registry.MERGE)
          .name("udf")
          .persist()
          .build();
      registry = storeProvider.getOrCreateStore(registrationConfig);
      registry.putIfAbsent(REGISTRY, Registry.getDefaultInstance());
    } catch (StoreException e) {
      throw new DrillRuntimeException("Failure while loading remote registry.", e);
    }

    TransientStoreConfig<String> unregistrationConfig = TransientStoreConfig.
        newJacksonBuilder(mapper, String.class).name("udf/unregister").build();
    unregistration = coordinator.getOrCreateTransientStore(unregistrationConfig);
    unregistration.addListener(unregistrationListener);

    TransientStoreConfig<String> jarsConfig = TransientStoreConfig.
        newJacksonBuilder(mapper, String.class).name("udf/jars").build();
    jars = coordinator.getOrCreateTransientStore(jarsConfig);

    this.retryTimes = config.getInt(ExecConstants.UDF_RETRY_TIMES);

    Configuration conf = new Configuration();
    if (config.hasPath(ExecConstants.UDF_DIRECTORY_FS)) {
      conf.set(FileSystem.FS_DEFAULT_NAME_KEY, config.getString(ExecConstants.UDF_DIRECTORY_FS));
    }

    try {
      this.fs = FileSystem.get(conf);
      String base = config.getString(ExecConstants.UDF_DIRECTORY_BASE);
      this.registryArea = createArea(fs, base, config.getString(ExecConstants.UDF_DIRECTORY_REGISTRY));
      this.stagingArea = createArea(fs, base, config.getString(ExecConstants.UDF_DIRECTORY_STAGING));
      this.tmpArea = createArea(fs, base, config.getString(ExecConstants.UDF_DIRECTORY_TMP));
    } catch (IOException e) {
      throw new DrillRuntimeException("Error during areas creation for remote function registry.", e);
    }
  }

  public Registry getRegistry() {
    return registry.get(REGISTRY);
  }

  public Registry getRegistry(DataChangeVersion version) {
    return registry.get(REGISTRY, version);
  }

  public void updateRegistry(Registry registryContent, DataChangeVersion version) throws VersionMismatchException {
    registry.put(REGISTRY, registryContent, version);
  }

  public void submitForUnregistration(String jar) {
    putIfAbsent(unregistration, jar, jar);
  }

  public void finishUnregistration(String jar) {
    remove(unregistration, jar);
  }

  public String addToJars(String jar, Action action) {
    return putIfAbsent(jars, jar, action.toString());
  }

  public void removeFromJars(String jar) {
    remove(jars, jar);
  }

  public int getRetryTimes() {
    return retryTimes;
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

  private Path createArea(FileSystem fs, String base, String directory) throws IOException {
    String dir = FilenameUtils.concat(base, directory);
    Path path = new Path(dir);
    fs.mkdirs(path);
    return path;
  }

  private <V> V putIfAbsent(TransientStore<V> store, String key, V value) {
    return store.putIfAbsent(key, value);
  }

  private <V> void remove(TransientStore<V> store, String key) {
    store.remove(key);
  }

  @Override
  public void close() throws Exception {
    if (fs != null) {
      fs.close();
    }
  }

  public enum Action {
    REGISTRATION,
    UNREGISTRATION
  }

}
