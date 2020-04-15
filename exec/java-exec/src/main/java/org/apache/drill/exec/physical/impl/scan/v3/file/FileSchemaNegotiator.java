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

import org.apache.drill.exec.physical.impl.scan.v3.SchemaNegotiator;
import org.apache.drill.exec.store.dfs.DrillFileSystem;
import org.apache.drill.exec.store.dfs.easy.FileWork;
import org.apache.hadoop.mapred.FileSplit;

/**
 * The file schema negotiator provides access to the Drill file system
 * and to the file split which the reader is to consume.
 */
public interface FileSchemaNegotiator extends SchemaNegotiator {

  /**
   * Gives the Drill file system for this operator.
   */
  DrillFileSystem fileSystem();

  /**
   * Describes the file split (path and block offset) for this scan.
   *
   * @return Hadoop file split object with the file path, block
   * offset, and length.
   */
  FileSplit split();

  /**
   * Returns Drill's version of the Hadoop file split.
   */
  FileWork fileWork();
}
