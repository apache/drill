/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.drill.exec.store.xml;

import java.util.Vector;

public class XMLDataVector {
  private Vector<String> keys;

  private Vector data;

  private boolean isArray;

  private String nestedFieldName;

  public XMLDataVector() {
    keys = new Vector();
    data = new Vector();
    nestedFieldName = "";
    isArray = false;
  }

  public boolean containsKey(String k) {
    return keys.contains(k);
  }

  public boolean is_array() {
    if (data.size() == 1) {
      return true;
    } else {
      return isArray;
    }
  }

  public boolean isNested() {
    return false;
  }

  public boolean add(XMLDataObject e) {

    if (keys.contains(e.key)) {
      isArray = true;
    } else {
      keys.add(e.key);
    }

    return data.add(e);
  }

  public void empty() {
    keys = new Vector();
    data = new Vector();
    nestedFieldName = "";
    isArray = false;
  }

  public Vector getDataVector() {
    return data;
  }

  public String getNestedFieldName() {
    return nestedFieldName;
  }

  public void setNestedFieldName(String s) {
    nestedFieldName = s;
  }
}
