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
package org.apache.drill.exec.store.hbase;

import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.common.types.Types;

public interface DrillHBaseConstants {
  public static final String ROW_KEY = "row_key";

  public static final SchemaPath ROW_KEY_PATH = SchemaPath.getSimplePath(ROW_KEY);

  public static final String HBASE_ZOOKEEPER_PORT = "hbase.zookeeper.property.clientPort";

  public static final MajorType ROW_KEY_TYPE = Types.required(MinorType.VARBINARY);

  public static final MajorType COLUMN_FAMILY_TYPE = Types.required(MinorType.MAP);

  public static final MajorType COLUMN_TYPE = Types.optional(MinorType.VARBINARY);

  public static final String SYS_STORE_PROVIDER_HBASE_TABLE = "drill.exec.sys.store.provider.hbase.table";

  public static final String SYS_STORE_PROVIDER_HBASE_CONFIG = "drill.exec.sys.store.provider.hbase.config";
}
