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
package org.apache.drill.exec.coord.store;

import org.apache.drill.exec.store.sys.PersistentStore;

/**
 * Factory that is used to obtain a {@link TransientStore store} instance.
 */
public interface TransientStoreFactory extends AutoCloseable {

  /**
   * Returns a {@link TransientStore transient store} instance for the given configuration.
   *
   * Note that implementors have liberty to cache previous {@link PersistentStore store} instances.
   *
   * @param config  store configuration
   * @param <V>  store value type
   */
  <V> TransientStore<V> getOrCreateStore(TransientStoreConfig<V> config);
}
