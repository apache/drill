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
package org.apache.drill.exec.physical.impl.window;

import org.eigenbase.sql.SqlCall;
import org.eigenbase.sql.SqlKind;
import org.eigenbase.sql.SqlNode;
import org.eigenbase.sql.SqlOperator;
import org.eigenbase.sql.util.SqlBasicVisitor;
import org.eigenbase.util.Util;

/**
 * Visitor which looks for an over clause inside a tree of {@link
 * SqlNode} objects.
 */
public class OverFinder extends SqlBasicVisitor<Void> {

  public boolean findOver(SqlNode node) {
    try {
      node.accept(this);
      return false;
    } catch (Util.FoundOne e) {
      Util.swallow(e, null);
      return true;
    }
  }

  @Override
  public Void visit(SqlCall call) {
    final SqlOperator operator = call.getOperator();

    if (operator.getKind().equals(SqlKind.OVER)) {
      throw new Util.FoundOne(call);
    }

    return super.visit(call);
  }
}