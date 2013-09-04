/*******************************************************************************
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
 ******************************************************************************/
package org.apache.drill.optiq;

import net.hydromatic.optiq.rules.java.EnumerableConvention;

import org.apache.drill.exec.client.DrillClient;
import org.apache.drill.jdbc.DrillHandler;
import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.convert.ConverterRule;

/**
 * Rule that converts any Drill relational expression to enumerable format by adding a {@link EnumerableDrillRel}.
 */
public class EnumerableDrillRule extends ConverterRule {

  private final DrillClient client;
  
  public static EnumerableDrillRule getInstance(DrillClient client){
    return new EnumerableDrillRule(EnumerableConvention.INSTANCE, client);
  }
  
  private EnumerableDrillRule(EnumerableConvention outConvention, DrillClient client) {
    super(RelNode.class, DrillRel.CONVENTION, outConvention, "EnumerableDrillRule." + outConvention);
    this.client = client;
  }

  @Override
  public boolean isGuaranteed() {
    return true;
  }

  @Override
  public RelNode convert(RelNode rel) {
    assert rel.getTraitSet().contains(DrillRel.CONVENTION);
    return new EnumerableDrillRel(client, rel.getCluster(), rel.getTraitSet().replace(getOutConvention()), rel);
  }
}
