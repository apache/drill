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
package com.fasterxml.jackson.core.json;

/**
 * Exposes certain package protected methods in Jackson.
 */
public class JsonReadContextExposer {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(JsonReadContextExposer.class);

  public static int getColNmbr(JsonReadContext c){
    return c._columnNr;
  }

  public static int getLineNmbr(JsonReadContext c){
    return c._lineNr;
  }


  public static void reset(JsonReadContext c, int type, int lineNr, int colNr){
    c.reset(type, lineNr, colNr);
  }

}
