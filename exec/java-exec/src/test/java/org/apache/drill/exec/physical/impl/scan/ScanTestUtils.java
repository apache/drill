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
package org.apache.drill.exec.physical.impl.scan;

import java.util.List;

import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.physical.impl.scan.project.ResolvedColumn;
import org.apache.drill.exec.physical.impl.scan.project.ResolvedTuple;
import org.apache.drill.exec.physical.impl.scan.project.ScanLevelProjection.ScanProjectionParser;
import org.apache.drill.exec.physical.impl.scan.project.SchemaLevelProjection.SchemaProjectionResolver;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.record.metadata.TupleSchema;
import org.apache.drill.shaded.guava.com.google.common.collect.ImmutableList;

public class ScanTestUtils {

  /**
   * Type-safe way to define a list of parsers.
   * @param parsers as a varArgs list convenient for testing
   * @return parsers as a Java List for input to the scan
   * projection framework
   */

  public static List<ScanProjectionParser> parsers(ScanProjectionParser... parsers) {
    return ImmutableList.copyOf(parsers);
  }

  public static List<SchemaProjectionResolver> resolvers(SchemaProjectionResolver... resolvers) {
    return ImmutableList.copyOf(resolvers);
  }

  public static TupleMetadata schema(ResolvedTuple output) {
    final TupleMetadata schema = new TupleSchema();
    for (final ResolvedColumn col : output.columns()) {
      MaterializedField field = col.schema();
      if (field.getType() == null) {

        // Convert from internal format of null columns (unset type)
        // to a usable form (explicit minor type of NULL.)

        field = MaterializedField.create(field.getName(),
            Types.optional(MinorType.NULL));
      }
      schema.add(field);
    }
    return schema;
  }
}
