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
package org.apache.drill.exec.store.easy.json;

/**
 * Enhanced second-generation JSON loader which takes an input
 * source and creates a series of record batches using the
 * {@link ResultSetLoader} abstraction.
 */

public interface JsonLoader {

  /**
   * Read one record of data.
   *
   * @return <true> if a record was loaded, <false> if EOF.
   * @throws UserException for most errors
   * @throws RuntimeException for unexpected errors, most often due
   * to code errors
   */

  boolean next();

  /**
   * Indicates that a batch is complete. Tells the loader to materialize
   * any deferred null fields.
   */

  void endBatch();

  /**
   * Releases resources held by this class, but does not close resources
   * passed into the class, such as the input stream or result set
   * loader.
   */

  void close();
}
