/**
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
package org.apache.drill.exec.planner.physical;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.List;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.rules.ProjectRemoveRule;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.drill.exec.planner.physical.PrelUtil.ProjectPushInfo;
import org.apache.drill.exec.planner.types.DrillRelDataTypeSystem;
import org.junit.Test;

import com.google.common.collect.Lists;

/**
 * Unit test for @{code PrelUtil}
 */
public class TestPrelUtil {
  private static final RelDataType ANY_TYPE = new BasicSqlType(DrillRelDataTypeSystem.DRILL_REL_DATATYPE_SYSTEM, SqlTypeName.ANY);

  @Test
  public void testGetColumns() {
    // Based on DRILL-4334 test failure. Field chosen to make it fail for both Java7 and Java8.
    RelDataType rowType = new RelRecordType(Arrays.<RelDataTypeField> asList(
        new RelDataTypeFieldImpl("*", 0, ANY_TYPE),
        new RelDataTypeFieldImpl("rating", 1, ANY_TYPE),
        new RelDataTypeFieldImpl("full_name", 2, ANY_TYPE),
        new RelDataTypeFieldImpl("firstname", 3, ANY_TYPE)
        ));

    List<RexNode> projects = Arrays.<RexNode>asList(new RexInputRef(1, ANY_TYPE), new RexInputRef(3, ANY_TYPE), new RexInputRef(2, ANY_TYPE));
    ProjectPushInfo info = PrelUtil.getColumns(rowType, projects);

    // Making sure that fields are ordered the same as the projects
    for(int index = 0; index < info.desiredFields.size(); index++) {
      assertEquals(((RexInputRef) projects.get(index)).getIndex(), info.desiredFields.get(index).origIndex);
    }

    // Create a new projection, and check it's the identity.
    RelDataType newRowType = info.createNewRowType(new JavaTypeFactoryImpl());
    List<RexNode> newProjects = Lists.newArrayList();
    for (RexNode n : projects) {
      newProjects.add(n.accept(info.getInputRewriter()));
    }

    assertTrue("Projection should be identity", ProjectRemoveRule.isIdentity(newProjects, newRowType));
  }

}
