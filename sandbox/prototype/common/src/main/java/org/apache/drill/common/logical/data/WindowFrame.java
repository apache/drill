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
package org.apache.drill.common.logical.data;

import org.apache.drill.common.expression.FieldReference;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

@JsonTypeName("window-frame")
public class WindowFrame {
  
  public static final String ALL = "all";
  public static final String HERE = "here";
  
  private final FieldReference[] keys;
  private final FieldReference ref;
  private final int before;
  private final int after;
  

  @JsonCreator
  public WindowFrame(@JsonProperty("keys") FieldReference[] keys, @JsonProperty("ref") FieldReference ref, @JsonProperty("before") String before, @JsonProperty("after") String after) {
    super();
    this.keys = keys;
    this.ref = ref;
    this.before = getVal(before);
    this.after = getVal(after);
  }

  private int getVal(String phrase){
    if(ALL.equals(phrase)) return -1;
    if(HERE.equals(phrase)) return 0;
    return Integer.parseInt(phrase);
  }

  private String getStr(int val){
    if(val == 0) return HERE;
    if(val == -1) return ALL;
    return Integer.toString(val);
  }

  public static String getAll() {
    return ALL;
  }


  public static String getHere() {
    return HERE;
  }


  public FieldReference[] getKeys() {
    return keys;
  }


  public FieldReference getRef() {
    return ref;
  }


  public String getBefore() {
    return getStr(before);
  }


  public String getAfter() {
    return getStr(after);
  }
  
  
}
