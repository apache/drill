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
package org.apache.drill.exec.rpc;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundMessageHandlerAdapter;

import java.io.OutputStream;

import org.apache.drill.exec.proto.GeneralRPCProtos.CompleteRpcMessage;
import org.apache.drill.exec.proto.GeneralRPCProtos.RpcHeader;

import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.WireFormat;

/**
 * Converts an RPCMessage into wire format.
 */
class RpcEncoder extends ChannelOutboundMessageHandlerAdapter<OutboundRpcMessage>{
  final org.slf4j.Logger logger;
  
  static final int HEADER_TAG = makeTag(CompleteRpcMessage.HEADER_FIELD_NUMBER, WireFormat.WIRETYPE_LENGTH_DELIMITED);
  static final int PROTOBUF_BODY_TAG = makeTag(CompleteRpcMessage.PROTOBUF_BODY_FIELD_NUMBER, WireFormat.WIRETYPE_LENGTH_DELIMITED);
  static final int RAW_BODY_TAG = makeTag(CompleteRpcMessage.RAW_BODY_FIELD_NUMBER, WireFormat.WIRETYPE_LENGTH_DELIMITED);
  static final int HEADER_TAG_LENGTH = getRawVarintSize(HEADER_TAG);
  static final int PROTOBUF_BODY_TAG_LENGTH = getRawVarintSize(PROTOBUF_BODY_TAG);
  static final int RAW_BODY_TAG_LENGTH = getRawVarintSize(RAW_BODY_TAG);
  
  public RpcEncoder(String name){
    this.logger = org.slf4j.LoggerFactory.getLogger(RpcEncoder.class.getCanonicalName() + "." + name);
  }
  
  @Override
  public void flush(ChannelHandlerContext ctx, OutboundRpcMessage msg) throws Exception {
    if(!ctx.channel().isOpen()){
      return;
    }
    
    try{
      if(RpcConstants.EXTRA_DEBUGGING) logger.debug("Encoding outbound message {}", msg);
      // first we build the RpcHeader 
      RpcHeader header = RpcHeader.newBuilder() //
          .setMode(msg.mode) //
          .setCoordinationId(msg.coordinationId) //
          .setRpcType(msg.rpcType).build();
      
      // figure out the full length
      int headerLength = header.getSerializedSize();
      int protoBodyLength = msg.pBody.getSerializedSize();
      int rawBodyLength = msg.getRawBodySize();
      int fullLength = //
          HEADER_TAG_LENGTH + getRawVarintSize(headerLength) + headerLength +   //
          PROTOBUF_BODY_TAG_LENGTH + getRawVarintSize(protoBodyLength) + protoBodyLength; //
      
      if(rawBodyLength > 0){
        fullLength += (RAW_BODY_TAG_LENGTH + getRawVarintSize(rawBodyLength) + rawBodyLength);
      }

      // set up buffers.
      ByteBuf buf = ctx.nextOutboundByteBuffer();
      OutputStream os = new ByteBufOutputStream(buf);
      CodedOutputStream cos = CodedOutputStream.newInstance(os);

      // write full length first (this is length delimited stream).
      cos.writeRawVarint32(fullLength);
      
      // write header
      cos.writeRawVarint32(HEADER_TAG);
      cos.writeRawVarint32(headerLength);
      header.writeTo(cos);

      // write protobuf body length and body
      cos.writeRawVarint32(PROTOBUF_BODY_TAG);
      cos.writeRawVarint32(protoBodyLength);
      msg.pBody.writeTo(cos);

      // if exists, write data body and tag.
      // TODO: is it possible to avoid this copy, i think so...
      if(msg.getRawBodySize() > 0){
        if(RpcConstants.EXTRA_DEBUGGING) logger.debug("Writing raw body of size {}", msg.getRawBodySize());
        cos.writeRawVarint32(RAW_BODY_TAG);
        cos.writeRawVarint32(rawBodyLength);
        cos.flush(); // need to flush so that dbody goes after if cos is caching.
        for(int i =0; i < msg.dBodies.length; i++){
          buf.writeBytes(msg.dBodies[i]);  
        }
      }else{
        cos.flush();
      }
      if(RpcConstants.EXTRA_DEBUGGING) logger.debug("Wrote message with length header of {} bytes and body of {} bytes.", getRawVarintSize(fullLength), fullLength);
      if(RpcConstants.EXTRA_DEBUGGING) logger.debug("Sent message.  Ending writer index was {}.", buf.writerIndex());
    
    }finally{
      // make sure to release Rpc Messages unerlying byte buffers.
      msg.release();
    }
  }
  
  /** Makes a tag value given a field number and wire type, copied from WireFormat since it isn't public.  */
  static int makeTag(final int fieldNumber, final int wireType) {
    return (fieldNumber << 3) | wireType;
  }
  
  public static int getRawVarintSize(int value) {
    int count = 0;
    while (true) {
      if ((value & ~0x7F) == 0) {
        count++;
        return count;
      } else {
        count++;
        value >>>= 7;
      }
    }
  }
  
}
