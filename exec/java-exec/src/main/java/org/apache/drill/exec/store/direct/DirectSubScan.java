/*
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
package org.apache.drill.exec.store.direct;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.drill.exec.physical.base.AbstractSubScan;
import org.apache.drill.exec.proto.UserBitShared.CoreOperatorType;
import org.apache.drill.exec.store.RecordReader;

import com.fasterxml.jackson.annotation.JsonTypeInfo;

import static com.fasterxml.jackson.annotation.JsonTypeInfo.Id.NAME;
import static com.fasterxml.jackson.annotation.JsonTypeInfo.As.WRAPPER_OBJECT;

@JsonTypeName("direct-sub-scan")
public class DirectSubScan extends AbstractSubScan {

  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DirectSubScan.class);
  @JsonTypeInfo(use=NAME, include=WRAPPER_OBJECT)
  private final RecordReader reader;

  @JsonCreator
  public DirectSubScan(@JsonProperty("reader") RecordReader reader) {
    super(null);
    this.reader = reader;
  }

  @JsonProperty
  //@JsonGetter("reader")
  public RecordReader getReader() {
    return reader;
  }

  @Override
  public int getOperatorType() {
    return CoreOperatorType.DIRECT_SUB_SCAN_VALUE;
  }


}
