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
package org.apache.drill.exec.resourcemgr;

public final class RMCommonDefaults {

  public static final int MAX_ADMISSIBLE_DEFAULT = 10;

  public static final int MAX_WAITING_DEFAULT = 10;

  public static final int MAX_WAIT_TIMEOUT_IN_MS_DEFAULT = 30000;

  public static final boolean WAIT_FOR_PREFERRED_NODES_DEFAULT = true;

  public static final double ROOT_POOL_DEFAULT_MEMORY_PERCENT = 0.9;

  public static final String ROOT_POOL_DEFAULT_QUEUE_SELECTION_POLICY = "bestfit";

}
