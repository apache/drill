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

  package org.apache.drill.exec.store.msaccess;

  import com.fasterxml.jackson.annotation.JsonCreator;
  import com.fasterxml.jackson.annotation.JsonInclude;
  import com.fasterxml.jackson.annotation.JsonProperty;
  import com.fasterxml.jackson.annotation.JsonTypeName;

  import org.apache.drill.common.PlanStringBuilder;
  import org.apache.drill.common.logical.FormatPluginConfig;
  import org.apache.drill.common.types.TypeProtos;
  import org.apache.drill.shaded.guava.com.google.common.collect.ImmutableList;
  import org.apache.drill.exec.store.msaccess.MsaccessBatchReader.MsaccessReaderConfig;

  import java.util.Collections;
  import java.util.List;
  import java.util.Objects;

  @JsonTypeName(MsaccessFormatPlugin.DEFAULT_NAME)
  @JsonInclude(JsonInclude.Include.NON_DEFAULT)

  public class MsaccessFormatConfig implements FormatPluginConfig {
    //declare each column header as private final vartype
    private final List<String> extensions;

    private final List<String> tableNames;

    private final TypeProtos.MinorType dataType;

    private final String headerName;

    private final String dateTimeFormat;

    private final String currencyFormat;

    // Omitted properties take reasonable defaults
    @JsonCreator
    //list each column header as jsonProperty, vartype, varname
    public MsaccessFormatConfig(@JsonProperty("extensions") List<String> extensions, @JsonProperty("tableNames") List<String> tableNames, @JsonProperty("dataType") TypeProtos.MinorType dataType, @JsonProperty("headerName") String headerName, @JsonProperty("dateTimeFormat") String dateTimeFormat, @JsonProperty("currencyFormat") String currencyFormat) {
      this.extensions = extensions == null ? Collections.singletonList("accdb") : ImmutableList.copyOf(extensions);
      this.tableNames = tableNames;
      this.dataType = dataType;
      this.headerName = headerName;
      this.dateTimeFormat = dateTimeFormat;
      this.currencyFormat = currencyFormat;

    }

    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    //accessor method for each column header
    public List<String> getExtensions() {
      return extensions;
    }

    public List<String> getTableNames() {
      return tableNames;
    }

    public TypeProtos.MinorType getDataType() {
      return dataType;
    }

    public String getHeaderName() {
      return headerName;
    }

    public String getDateTimeFormat() {
      return dateTimeFormat;
    }

    public String getCurrencyFormat() {
      return currencyFormat;
    }

    public MsaccessReaderConfig getReaderConfig(MsaccessFormatPlugin plugin) {
      MsaccessReaderConfig readerConfig = new MsaccessReaderConfig(plugin);
      return readerConfig;
    }

    @Override
    //include all variables in .hash()
    public int hashCode() {
      return Objects.hash(extensions, tableNames, dataType, headerName, dateTimeFormat, currencyFormat);
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null || getClass() != obj.getClass()) {
        return false;
      }
      //return && Objects.equals()... for each coluumn header
      MsaccessFormatConfig other = (MsaccessFormatConfig) obj;
      return Objects.equals(extensions, other.extensions) && Objects.equals(tableNames, other.tableNames) && Objects.equals(dataType, other.dataType) && Objects.equals(headerName, other.headerName) && Objects.equals(dateTimeFormat, other.dateTimeFormat) && Objects.equals(currencyFormat, other.currencyFormat);
    }

    @Override
    //.field for each column header
    public String toString() {
      return new PlanStringBuilder(this).field("extensions", extensions).field("tableNames", tableNames).field("dataType", dataType).field("headerName", headerName).field("dateTimeFormat", dateTimeFormat).field("currencyFormat", currencyFormat).toString();
    }
  }
