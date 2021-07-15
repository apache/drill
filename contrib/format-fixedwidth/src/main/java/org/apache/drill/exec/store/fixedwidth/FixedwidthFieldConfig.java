package org.apache.drill.exec.store.fixedwidth;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.drill.common.types.TypeProtos;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonTypeName;

@JsonTypeName("fixedwidthReaderFieldDescription")
@JsonInclude(JsonInclude.Include.NON_DEFAULT)
public class FixedwidthFieldConfig {

  private final TypeProtos.MinorType dataType;
  private final String fieldName;
  private final String dateTimeFormat;
  private final int startIndex;
  private final int fieldWidth;

  public FixedwidthFieldConfig(@JsonProperty("dataType") TypeProtos.MinorType dataType,
                               @JsonProperty("fieldName") String fieldName,
                               @JsonProperty("dateTimeFormat") String dateTimeFormat,
                               @JsonProperty("startIndex") int startIndex,
                               @JsonProperty("fieldWidth") int fieldWidth) {
    this.dataType = dataType;
    this.fieldName = fieldName;
    this.dateTimeFormat = dateTimeFormat;
    this.startIndex = startIndex;
    this.fieldWidth = fieldWidth;
  }

  public TypeProtos.MinorType getDataType(){
    return dataType;
  }

//  public void setDataType(TypeProtos.MinorType dataType){
//    this.dataType = dataType;
//  }

  public String getFieldName(){
    return fieldName;
  }

//  public void setFieldName(String fieldName){
//    this.fieldName = fieldName;
//  }

  public String getDateTimeFormat() {
    return dateTimeFormat;
  }

//  public void setDateTimeFormat(String dateTimeFormat) {
//    this.dateTimeFormat = dateTimeFormat;
//  }

  public int getStartIndex(){
    return startIndex;
  }

//  public void setStartIndex(int startIndex){
//    this.startIndex = startIndex;
//  }

  public int getFieldWidth(){
    return fieldWidth;
  }

//  public void setFieldWidth(int fieldWidth){
//    this.fieldWidth = fieldWidth;
//  }

}
