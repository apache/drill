package com.google.protobuf;

public class WireFormatProxy {

  WireFormatProxy(){};
  
  public static int getFieldType(int tag){
    return WireFormat.getTagWireType(tag);
  }
}
