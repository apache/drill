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
/**
 * Provides a set of base classes for creating a storage plugin.
 * Handles the "boilerplate" which is otherwise implemented via
 * copy-and-paste.
 * <p>
 * The simplest possible plugin will use many of the base
 * classes as-is, and will implement:
 * <ul>
 * <li>The storage plugin configuration (needed to identify the plugin),</li>
 * <li>The storage plugin class,</li>
 * <li>The schema factory for the plugin (which says which schemas
 * or tables are available),<.li>
 * <li>The batch reader to read the data for the plugin.</li>
 * </ul>
 *
 * Super classes require a number of standard methods to make
 * copies, present configuration and so on. As much as possible,
 * this class handles those details. For example, the
 * {@link StoragePluginOptions} class holds many of the options
 * that otherwise require one-line method implementations.
 * The framework automatically makes copies of scan objects
 * to avoid other standard methods.
 * <p>
 * As a plugin gets more complex, it can create its own
 * group and sub scans, add filter push down, and so on.
 *
 * @see {@link DummyStoragePlugin} for an example how this
 * framework is used. The Dummy plugin is the "test mule"
 * for this framework.
 */

package org.apache.drill.exec.store.base;
