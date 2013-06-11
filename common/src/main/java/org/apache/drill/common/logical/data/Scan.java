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
package org.apache.drill.common.logical.data;

import org.apache.drill.common.JSONOptions;
import org.apache.drill.common.expression.FieldReference;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.drill.common.logical.data.visitors.LogicalVisitor;


@JsonTypeName("scan")
public class Scan extends SourceOperator{
	private final String storageEngine;
	private final JSONOptions selection;
	private final FieldReference outputReference;
	
	@JsonCreator
  public Scan(@JsonProperty("storageengine") String storageEngine, @JsonProperty("selection") JSONOptions selection, @JsonProperty("ref") FieldReference outputReference) {
    super();
    this.storageEngine = storageEngine;
    this.selection = selection;
    this.outputReference = outputReference;
  }

  @JsonProperty("storageengine")
  public String getStorageEngine() {
    return storageEngine;
  }

  public JSONOptions getSelection() {
    return selection;
  }

  @JsonProperty("ref")
  public FieldReference getOutputReference() {
    return outputReference;
  }

  @Override
  public <T, X, E extends Throwable> T accept(LogicalVisitor<T, X, E> logicalVisitor, X value) throws E {
      return logicalVisitor.visitScan(this, value);
  }

  public static ScanBuilder builder() {
    return new ScanBuilder();
  }

}
