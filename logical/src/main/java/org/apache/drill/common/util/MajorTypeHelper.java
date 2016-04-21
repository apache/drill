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
package org.apache.drill.common.util;

import org.apache.arrow.vector.types.Types.DataMode;
import org.apache.arrow.vector.types.Types.MajorType;
import org.apache.arrow.vector.types.Types.MinorType;

import java.util.ArrayList;
import java.util.List;

public class MajorTypeHelper {

  public static MajorType getArrowMajorType(org.apache.drill.common.types.TypeProtos.MajorType drillMajorType) {
    return new MajorType(getArrowMinorType(drillMajorType.getMinorType()),
            getArrowDataMode(drillMajorType.getMode()),
            drillMajorType.getPrecision(),
            drillMajorType.getScale(),
            drillMajorType.getTimeZone(),
            drillMajorType.getWidth(),
            getArrowSubtypes(drillMajorType.getSubTypeList()));
  }

  public static MinorType getArrowMinorType(org.apache.drill.common.types.TypeProtos.MinorType drillMinorType) {
    return MinorType.valueOf(drillMinorType.name());
  }

  public static DataMode getArrowDataMode(org.apache.drill.common.types.TypeProtos.DataMode drillDataMode) {
    return DataMode.valueOf(drillDataMode.name());
  }

  public static List<MinorType> getArrowSubtypes(List<org.apache.drill.common.types.TypeProtos.MinorType> drillSubTypes) {
    if (drillSubTypes == null) {
      return null;
    }
    List<MinorType> arrowMinorTypes = new ArrayList<>();
    for (org.apache.drill.common.types.TypeProtos.MinorType drillMinorType : drillSubTypes) {
      arrowMinorTypes.add(getArrowMinorType(drillMinorType));
    }
    return arrowMinorTypes;
  }

  public static org.apache.drill.common.types.TypeProtos.MajorType getDrillMajorType(MajorType arrowMajorType) {
    org.apache.drill.common.types.TypeProtos.MajorType.Builder builder = org.apache.drill.common.types.TypeProtos.MajorType.newBuilder().setMinorType(getDrillMinorType(arrowMajorType.getMinorType()));
    if (arrowMajorType.getMode() != null) {
      builder.setMode(getDrillDataMode(arrowMajorType.getMode()));
    }
    builder.setPrecision(arrowMajorType.getPrecision())
            .setScale(arrowMajorType.getScale())
            .setTimeZone(arrowMajorType.getTimezone())
            .setWidth(arrowMajorType.getWidth());
    if (arrowMajorType.getSubTypes() != null) {
      for (MinorType subType : arrowMajorType.getSubTypes()) {
        builder.addSubType(getDrillMinorType(subType));
      }
    }
    return builder.build();
  }

  public static org.apache.drill.common.types.TypeProtos.MinorType getDrillMinorType(MinorType arrowMinorType) {
    return org.apache.drill.common.types.TypeProtos.MinorType.valueOf(arrowMinorType.name());
  }

  public static org.apache.drill.common.types.TypeProtos.DataMode getDrillDataMode(DataMode arrowDataMode) {
    return org.apache.drill.common.types.TypeProtos.DataMode.valueOf(arrowDataMode.name());
  }

}
