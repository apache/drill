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
package org.apache.drill.common.logical;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Logical plan meta properties.
 */
public class PlanProperties {
  public static enum PlanType {APACHE_DRILL_LOGICAL, APACHE_DRILL_PHYSICAL}

  public PlanType type;
  public int version;
	public Generator generator;

	@JsonInclude(Include.NON_NULL)
	public static class Generator{
		public String type;
		public String info;

    private Generator(@JsonProperty("type") String type, @JsonProperty("info") String info) {
      this.type = type;
      this.info = info;
    }
  }

  private PlanProperties(@JsonProperty("version") int version,
                         @JsonProperty("generator") Generator generator,
                         @JsonProperty("type") PlanType type) {
    this.version = version;
    this.generator = generator;
    this.type = type;
  }

  public static PlanPropertiesBuilder builder() {
    return new PlanPropertiesBuilder();
  }

  public static class PlanPropertiesBuilder {
    private int version;
    private Generator generator;
    private PlanType type;

    public PlanPropertiesBuilder type(PlanType type) {
      this.type = type;
      return this;
    }

    public PlanPropertiesBuilder version(int version) {
      this.version = version;
      return this;
    }

    public PlanPropertiesBuilder generator(String type, String info) {
      this.generator = new Generator(type, info);
      return this;
    }

    public PlanPropertiesBuilder generator(Generator generator) {
      this.generator = generator;
      return this;
    }

    public PlanProperties build() {
      return new PlanProperties(version, generator, type);
    }
  }
}
