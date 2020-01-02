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

package org.apache.drill.exec.store.cassandra;

import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.Types;

public interface DrillCassandraConstants {

    public static final TypeProtos.MajorType ROW_KEY_TYPE = Types.required(TypeProtos.MinorType.VARCHAR);

    public static final TypeProtos.MajorType COLUMN_TYPE = Types.optional(TypeProtos.MinorType.VARCHAR);

    public static final String CASSANDRA_CONFIG_HOSTS = "cassandra.hosts";

    public static final String CASSANDRA_CONFIG_PORT = "cassandra.port";
}
