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

package org.apache.drill.exec.store.fixedwidth;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.drill.common.PlanStringBuilder;
import org.apache.drill.common.types.TypeProtos;

import java.util.Objects;


@JsonTypeName("fixedwidthReaderFieldDescription")
@JsonInclude(JsonInclude.Include.NON_DEFAULT)
public class FixedwidthFieldConfig {

  private final String name;
  private final int index;
  private final int width;
  private final TypeProtos.MinorType type;
  private final String dateTimeFormat;

  public FixedwidthFieldConfig(@JsonProperty("name") String name,
                               @JsonProperty("index") int index,
                               @JsonProperty("width") int width,
                               @JsonProperty("type") TypeProtos.MinorType type,
                               @JsonProperty("dateTimeFormat") String dateTimeFormat) {

    this.name = name;
    this.index = index;
    this.width = width;
    this.type = type;
    this.dateTimeFormat = dateTimeFormat;

    // Need to verify names are different - where can we access all the names of other columns
//    if(name != null){
//      this.name = name;
//    } else{
//      throw new IllegalArgumentException("Invalid name"); //Is this the right way to throw an exception if blank? What about if not valid SQL?
//    }
//
//    if (index >= 0){
//      this.index = index;
//    } else {
//        throw new IllegalArgumentException("Index must be 0 or greater");
//    }
//
//    //Can modify this to be optional and be calculated based on start index of this field and next
//    this.width = width;
//
//    if (type == null){
//      this.type = TypeProtos.MinorType.VARCHAR;
//    } else {
//      this.type = type;
//    }
//    this.dateTimeFormat = dateTimeFormat; // No default required, null is allowed
  }

  public String getName() {return name;}

  public int getIndex() {return index;}

  public int getWidth() {return width;}

  public TypeProtos.MinorType getType() {return type;}

  public String getDateTimeFormat() {return dateTimeFormat;}

  @Override
  public int hashCode() {
    return Objects.hash(name, index, width, type,  dateTimeFormat);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    FixedwidthFieldConfig other = (FixedwidthFieldConfig) obj;
    return Objects.equals(name, other.name)
      && Objects.equals(index, other.index)
      && Objects.equals(width, other.width)
      && Objects.equals(type, other.type)
      && Objects.equals(dateTimeFormat, other.dateTimeFormat);
  }

  @Override
  public String toString() {
    return new PlanStringBuilder(this)
      .field("name", name)
      .field("index", index)
      .field("width", width)
      .field("type", type)
      .field("dateTimeFormat", dateTimeFormat)
      .toString();
  }
}
