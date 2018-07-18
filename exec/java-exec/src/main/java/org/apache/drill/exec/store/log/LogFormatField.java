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

package org.apache.drill.exec.store.log;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonTypeName;

@JsonTypeName("regexReaderFieldDescription")
@JsonInclude(JsonInclude.Include.NON_DEFAULT)
public class LogFormatField {

  /*
   * The three configuration options for a field are:
   * 1.  The field name
   * 2.  The data type (fieldType).  Field type defaults to VARCHAR if it is not specified
   * 3.  The format string which is used for date/time fields.  This field is ignored if used with a non
   * date/time field.
   * */

  private String fieldName = "";
  private String fieldType = "VARCHAR";
  private String format;

  //These will be used in the future for field validation and masking
  //public String validator;
  //public double minValue;
  //public double maxValue;


  public LogFormatField() {
  }

  //These constructors are used for unit testing
  public LogFormatField(String fieldName) {
    this(fieldName, null, null);
  }

  public LogFormatField(String fieldName, String fieldType) {
    this(fieldName, fieldType, null);
  }

  public LogFormatField(String fieldName, String fieldType, String format) {
    this.fieldName = fieldName;
    this.fieldType = fieldType;
    this.format = format;
  }

  public String getFieldName() {
    return fieldName;
  }

  public String getFieldType() {
    return fieldType;
  }

  public String getFormat() {
    return format;
  }


  /*
  public String getValidator() { return validator; }

  public double getMinValue() { return minValue; }

  public double getMaxValue() {
    return maxValue;
  }
  */
}
