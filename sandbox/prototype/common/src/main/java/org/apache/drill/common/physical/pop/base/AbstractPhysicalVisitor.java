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
package org.apache.drill.common.physical.pop.base;

import org.apache.drill.common.physical.pop.Filter;
import org.apache.drill.common.physical.pop.PartitionToRandomExchange;
import org.apache.drill.common.physical.pop.Project;
import org.apache.drill.common.physical.pop.Sort;

public abstract class AbstractPhysicalVisitor<T, X, E extends Throwable> implements PhysicalVisitor<T, X, E> {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(AbstractPhysicalVisitor.class);

  @Override
  public T visitExchange(Exchange exchange, X value) throws E{
    return visitUnknown(exchange, value);
  }

  @Override
  public T visitFilter(Filter filter, X value) throws E{
    return visitUnknown(filter, value);
  }

  @Override
  public T visitProject(Project project, X value) throws E{
    return visitUnknown(project, value);
  }

  @Override
  public T visitSort(Sort sort, X value) throws E{
    return visitUnknown(sort, value);
  }

  @Override
  public T visitSender(Sender sender, X value) throws E {
    return visitUnknown(sender, value);
  }

  @Override
  public T visitReceiver(Receiver receiver, X value) throws E {
    return visitUnknown(receiver, value);
  }

  @Override
  public T visitScan(Scan<?> scan, X value) throws E{
    return visitUnknown(scan, value);
  }

  @Override
  public T visitStore(Store store, X value) throws E{
    return visitUnknown(store, value);
  }

  @Override
  public T visitPartitionToRandomExchange(PartitionToRandomExchange partitionToRandom, X value) throws E{
    return visitExchange(partitionToRandom, value);
  }

  @Override
  public T visitUnknown(PhysicalOperator op, X value) throws E{
    throw new UnsupportedOperationException(String.format(
        "The PhysicalVisitor of type %s does not currently support visiting the PhysicalOperator type %s.", this
            .getClass().getCanonicalName(), op.getClass().getCanonicalName()));
  }

}
