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
package org.apache.drill.exec.store.sys.zk;

import static org.apache.drill.exec.ExecConstants.DRILL_SYS_FILE_SUFFIX;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.commons.io.IOUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.drill.exec.store.dfs.shim.DrillFileSystem;
import org.apache.drill.exec.store.dfs.shim.DrillInputStream;
import org.apache.drill.exec.store.dfs.shim.DrillOutputStream;
import org.apache.drill.exec.store.sys.PStore;
import org.apache.drill.exec.store.sys.PStoreConfig;
import org.apache.hadoop.fs.Path;
import org.apache.zookeeper.CreateMode;

import com.google.common.base.Preconditions;

/**
 * Implementation of PStore using Zookeeper's PERSISTENT node.
 * @param <V>
 */
public class ZkPStore<V> extends ZkAbstractStore<V> implements PStore<V>{

  private DrillFileSystem fs;
  private Path blobPath;
  private boolean blobPathCreated;

  ZkPStore(CuratorFramework framework, DrillFileSystem fs, Path blobRoot, PStoreConfig<V> config)
      throws IOException {
    super(framework, config);

    this.fs = fs;
    this.blobPath = new Path(blobRoot, config.getName());
    this.blobPathCreated = false;
  }

  @Override
  public void createNodeInZK(String key, V value) {
    try {
      framework.create().withMode(CreateMode.PERSISTENT).forPath(p(key), config.getSerializer().serialize(value));
    } catch (Exception e) {
      throw new RuntimeException("Failure while accessing Zookeeper", e);
    }
  }

  @Override
  public boolean putIfAbsent(String key, V value) {
    try {
      if (framework.checkExists().forPath(p(key)) != null) {
        return false;
      } else {
        framework.create().withMode(CreateMode.PERSISTENT).forPath(p(key), config.getSerializer().serialize(value));
        return true;
      }

    } catch (Exception e) {
      throw new RuntimeException("Failure while accessing Zookeeper", e);
    }
  }

  @Override
  public V getBlob(String key) {
    try (DrillInputStream is = fs.open(path(key))) {
      return config.getSerializer().deserialize(IOUtils.toByteArray(is.getInputStream()));
    } catch (FileNotFoundException e) {
      return null;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void putBlob(String key, V value) {
    try (DrillOutputStream os = fs.create(path(key))) {
      IOUtils.write(config.getSerializer().serialize(value), os.getOuputStream());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private Path path(String name) throws IOException {
    Preconditions.checkArgument(
        !name.contains("/") &&
        !name.contains(":") &&
        !name.contains(".."));

    if (!blobPathCreated) {
      fs.mkdirs(blobPath);
      blobPathCreated = true;
    }

    return new Path(blobPath, name + DRILL_SYS_FILE_SUFFIX);
  }

}
