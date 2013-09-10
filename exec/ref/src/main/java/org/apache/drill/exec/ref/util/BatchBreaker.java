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
package org.apache.drill.exec.ref.util;

import org.apache.drill.exec.ref.RecordPointer;

public interface BatchBreaker {
  public boolean shouldBreakAfter(RecordPointer record);
  
  public static class CountBreaker implements BatchBreaker{
    private final int max;
    private int count = 0;
    
    public CountBreaker(int max) {
      super();
      this.max = max;
    }

    @Override
    public boolean shouldBreakAfter(RecordPointer record) {
      count++;
      if(count > max){
        count = 0;
        return true;
      }{
        return false;
      }
    }
    
  }
}
