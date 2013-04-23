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
package org.apache.drill.exec;

import java.util.Arrays;

import com.google.common.base.Charsets;
import com.google.common.primitives.Bytes;
import com.google.common.primitives.UnsignedBytes;

public class ByteReorder {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ByteReorder.class);
  
  public static void main(String[] args){
    String[] strings = {"hello", "goodbye", "my friend"};
    byte[][] bytes = new byte[strings.length][];
    for(int i =0; i < strings.length; i++){
      bytes[i] = strings[i].getBytes(Charsets.UTF_8);
    }
    
    for(int i =0; i < bytes.length; i++){
      for(int v = 0; v < bytes[i].length; v++){
        bytes[i][v] = (byte) ~bytes[i][v];
      }
    }
    
    Arrays.sort(bytes, UnsignedBytes.lexicographicalComparator());

    for(int i =0; i < bytes.length; i++){
      for(int v = 0; v < bytes[i].length; v++){
        bytes[i][v] = (byte) ~bytes[i][v];
      }
    }

    for(int i =0; i < bytes.length; i++){
      System.out.println(new String(bytes[i]));
    }
  }
}
