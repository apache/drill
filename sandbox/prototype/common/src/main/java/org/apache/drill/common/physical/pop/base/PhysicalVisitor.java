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

public interface PhysicalVisitor<RETURN, EXTRA, EXCEP extends Throwable> {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(PhysicalVisitor.class);
  
  
  public RETURN visitExchange(Exchange exchange, EXTRA value) throws EXCEP;
  public RETURN visitScan(Scan<?> scan, EXTRA value) throws EXCEP;
  public RETURN visitStore(Store store, EXTRA value) throws EXCEP;

  public RETURN visitFilter(Filter filter, EXTRA value) throws EXCEP;
  public RETURN visitProject(Project project, EXTRA value) throws EXCEP;
  public RETURN visitSort(Sort sort, EXTRA value) throws EXCEP;
  public RETURN visitSender(Sender sender, EXTRA value) throws EXCEP;
  public RETURN visitReceiver(Receiver receiver, EXTRA value) throws EXCEP;
  
  public RETURN visitUnknown(PhysicalOperator op, EXTRA value) throws EXCEP;
  
  public RETURN visitPartitionToRandomExchange(PartitionToRandomExchange partitionToRandom, EXTRA value) throws EXCEP; 

}
