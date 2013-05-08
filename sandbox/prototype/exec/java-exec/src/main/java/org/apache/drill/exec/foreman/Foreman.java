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
package org.apache.drill.exec.foreman;


public class Foreman extends Thread{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(Foreman.class);
  
  public Foreman(){
    
  }
  
  public void doWork(QueryWorkUnit work){
    // generate fragment structure. 
    // store fragments in distributed grid.
    // generate any codegen required and store in grid.
    // drop 
    // do get on the result set you're looking for.  Do the initial get on the result node you're looking for.  This will return either data or a metadata record set
  }

  public boolean checkStatus(long queryId){
    return false;
  }
}
