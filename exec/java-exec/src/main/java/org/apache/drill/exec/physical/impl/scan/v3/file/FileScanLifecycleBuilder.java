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
package org.apache.drill.exec.physical.impl.scan.v3.file;

import java.util.List;

import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.physical.impl.scan.v3.ScanLifecycleBuilder;
import org.apache.drill.exec.physical.impl.scan.v3.lifecycle.ScanLifecycle;
import org.apache.drill.exec.store.dfs.easy.FileWork;
import org.apache.drill.shaded.guava.com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

public class FileScanLifecycleBuilder extends ScanLifecycleBuilder {
  protected int maxPartitionDepth;
  protected boolean useLegacyWildcardExpansion;
  protected Path rootDir;
  private List<FileWork> splits;
  private Configuration fsConf;

  public void fileSystemConfig(Configuration fsConf) {
    this.fsConf = fsConf;
  }

  public void fileSplits(List<FileWork> splits) {
    this.splits = splits;
  }

  public void maxPartitionDepth(int maxPartitionDepth) {
    this.maxPartitionDepth = maxPartitionDepth;
  }

  public void useLegacyWildcardExpansion(boolean useLegacyWildcardExpansion) {
    this.useLegacyWildcardExpansion = useLegacyWildcardExpansion;
  }

  public void rootDir(Path rootDir) {
    this.rootDir = rootDir;
  }

  public List<FileWork> splits() {
    return Preconditions.checkNotNull(splits);
  }

  public Configuration fileSystemConfig() {
    if (fsConf == null) {
      fsConf = new Configuration();
    }
    return fsConf;
  }

  @Override
  public ScanLifecycle build(OperatorContext context) {
    return new FileScanLifecycle(context, this);
  }

  public int maxPartitionDepth() {
    return maxPartitionDepth;
  }

  public boolean useLegacyWildcardExpansion() {
    return useLegacyWildcardExpansion;
  }

  public Path rootDir() {
    return rootDir;
  }
}
