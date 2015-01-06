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


#include <iostream>
#include <google/protobuf/io/coded_stream.h>
#include "drill/common.hpp"
#include "rpcEncoder.hpp"
#include "rpcDecoder.hpp"
#include "rpcMessage.hpp"

namespace Drill{

// return the number of bytes we have read
int RpcDecoder::LengthDecode(const uint8_t* buf, uint32_t* p_length) {

    using google::protobuf::io::CodedInputStream;

    // read the frame to get the length of the message and then

    CodedInputStream* cis = new CodedInputStream(buf, 5); // read 5 bytes at most

    int pos0 = cis->CurrentPosition(); // for debugging
    cis->ReadVarint32(p_length);

    #ifdef CODER_DEBUG
    cerr << "p_length = " << *p_length << endl;
    #endif

    int pos1 = cis->CurrentPosition();

    #ifdef CODER_DEBUG
    cerr << "Reading full length " << *p_length << endl;
    #endif
    assert( (pos1-pos0) == getRawVarintSize(*p_length));
    delete cis;
    return (pos1-pos0);
}

// TODO: error handling
//
// - assume that the entire message is in the buffer and the buffer is constrained to this message
// - easy to handle with raw arry in C++
int RpcDecoder::Decode(const uint8_t* buf, int length, InBoundRpcMessage& msg) {
    using google::protobuf::io::CodedInputStream;

    // if(!ctx.channel().isOpen()){ return; }

    #ifdef  EXTRA_DEBUGGING
    std::cerr <<  "\nInbound rpc message received." << std::endl;
    #endif

    CodedInputStream* cis = new CodedInputStream(buf, length);


    int pos0 = cis->CurrentPosition(); // for debugging

    int len_limit = cis->PushLimit(length);

    uint32_t header_length = 0;
    cis->ExpectTag(RpcEncoder::HEADER_TAG);
    cis->ReadVarint32(&header_length);

    #ifdef CODER_DEBUG
    cerr << "Reading header length " << header_length << ", post read index " << cis->CurrentPosition() << endl;
    #endif

    exec::rpc::RpcHeader header;
    int header_limit = cis->PushLimit(header_length);
    header.ParseFromCodedStream(cis);
    cis->PopLimit(header_limit);
    msg.m_has_mode = header.has_mode();
    msg.m_mode = header.mode();
    msg.m_coord_id = header.coordination_id();
    msg.m_has_rpc_type = header.has_rpc_type();
    msg.m_rpc_type = header.rpc_type();

    //if(RpcConstants.EXTRA_DEBUGGING) logger.debug(" post header read index {}", buffer.readerIndex());

    // read the protobuf body into a buffer.
    cis->ExpectTag(RpcEncoder::PROTOBUF_BODY_TAG);
    uint32_t p_body_length = 0;
    cis->ReadVarint32(&p_body_length);

    #ifdef CODER_DEBUG
    cerr << "Reading protobuf body length " << p_body_length << ", post read index " << cis->CurrentPosition() << endl;
    #endif

    msg.m_pbody.resize(p_body_length);
    cis->ReadRaw(msg.m_pbody.data(),p_body_length);


    // read the data body.
    if (cis->BytesUntilLimit() > 0 ) {
    #ifdef CODER_DEBUG
        cerr << "Reading raw body, buffer has "<< cis->BytesUntilLimit() << " bytes available, current possion "<< cis->CurrentPosition()  << endl;
    #endif
        cis->ExpectTag(RpcEncoder::RAW_BODY_TAG);
        uint32_t d_body_length = 0;
        cis->ReadVarint32(&d_body_length);

        if(cis->BytesUntilLimit() != d_body_length) {
    #ifdef CODER_DEBUG
            cerr << "Expected to receive a raw body of " << d_body_length << " bytes but received a buffer with " <<cis->BytesUntilLimit() << " bytes." << endl;
    #endif
        }
        //msg.m_dbody.resize(d_body_length);
        //cis->ReadRaw(msg.m_dbody.data(), d_body_length);
        uint32_t currPos=cis->CurrentPosition();
        cis->GetDirectBufferPointer((const void**)&msg.m_dbody, (int*)&d_body_length);
        assert(msg.m_dbody==buf+currPos);
        cis->Skip(d_body_length);
    #ifdef CODER_DEBUG
        cerr << "Read raw body of " << d_body_length << " bytes" << endl;
    #endif
    } else {
    #ifdef CODER_DEBUG
        cerr << "No need to read raw body, no readable bytes left." << endl;
    #endif
    }
    cis->PopLimit(len_limit);


    // return the rpc message.
    // move the reader index forward so the next rpc call won't try to work with it.
    // buffer.skipBytes(dBodyLength);
    // messageCounter.incrementAndGet();
    #ifdef CODER_DEBUG
    cerr << "Inbound Rpc Message Decoded " << msg << endl;
    #endif

    int pos1 = cis->CurrentPosition();
    assert((pos1-pos0) == length);
    delete cis;
    return (pos1-pos0);
}

}//namespace Drill
