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


#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl_lite.h>
#include <google/protobuf/message_lite.h>
#include <google/protobuf/wire_format_lite.h>

#include "drill/common.hpp"
#include "rpcEncoder.hpp"
#include "rpcMessage.hpp"

namespace Drill{

using google::protobuf::internal::WireFormatLite;
using exec::rpc::CompleteRpcMessage;

const uint32_t RpcEncoder::HEADER_TAG = WireFormatLite::MakeTag(CompleteRpcMessage::kHeaderFieldNumber, WireFormatLite::WIRETYPE_LENGTH_DELIMITED);
const uint32_t RpcEncoder::PROTOBUF_BODY_TAG = WireFormatLite::MakeTag(CompleteRpcMessage::kProtobufBodyFieldNumber, WireFormatLite::WIRETYPE_LENGTH_DELIMITED);
const uint32_t RpcEncoder::RAW_BODY_TAG = WireFormatLite::MakeTag(CompleteRpcMessage::kRawBodyFieldNumber, WireFormatLite::WIRETYPE_LENGTH_DELIMITED);
const uint32_t RpcEncoder::HEADER_TAG_LENGTH = getRawVarintSize(HEADER_TAG);
const uint32_t RpcEncoder::PROTOBUF_BODY_TAG_LENGTH = getRawVarintSize(PROTOBUF_BODY_TAG);
const uint32_t RpcEncoder::RAW_BODY_TAG_LENGTH = getRawVarintSize(RAW_BODY_TAG);


bool RpcEncoder::Encode(DataBuf& buf, OutBoundRpcMessage& msg) {
    using exec::rpc::RpcHeader;
    using google::protobuf::io::CodedOutputStream;
    using google::protobuf::io::ArrayOutputStream;
    // Todo:
    //
    // - let a context manager to allocate a buffer `ByteBuf buf = ctx.alloc().buffer();`
    // - builder pattern
    //
    #ifdef CODER_DEBUG
    cerr << "\nEncoding outbound message " << msg << endl;
    #endif

    RpcHeader header;
    header.set_mode(msg.m_mode);
    header.set_coordination_id(msg.m_coord_id);
    header.set_rpc_type(msg.m_rpc_type);

    // calcute the length of the message
    int header_length = header.ByteSize();
    int proto_body_length = msg.m_pbody->ByteSize();
    int full_length = HEADER_TAG_LENGTH + getRawVarintSize(header_length) + header_length + \
                      PROTOBUF_BODY_TAG_LENGTH + getRawVarintSize(proto_body_length) + proto_body_length;

    /*
       if(raw_body_length > 0) {
       full_length += (RAW_BODY_TAG_LENGTH + getRawVarintSize(raw_body_length) + raw_body_length);
       }
       */

    buf.resize(full_length + getRawVarintSize(full_length));
    ArrayOutputStream* os = new ArrayOutputStream(buf.data(), buf.size());
    CodedOutputStream* cos = new CodedOutputStream(os);


    #ifdef CODER_DEBUG
    cerr << "Writing full length " << full_length << endl;
    #endif

    // write full length first (this is length delimited stream).
    cos->WriteVarint32(full_length);

    #ifdef CODER_DEBUG
    cerr << "Writing header length " << header_length << endl;
    #endif

    cos->WriteVarint32(HEADER_TAG);
    cos->WriteVarint32(header_length);

    header.SerializeToCodedStream(cos);

    // write protobuf body length and body
    #ifdef CODER_DEBUG
    cerr << "Writing protobuf body length " << proto_body_length << endl;
    #endif

    cos->WriteVarint32(PROTOBUF_BODY_TAG);
    cos->WriteVarint32(proto_body_length);
    msg.m_pbody->SerializeToCodedStream(cos);

    delete os;
    delete cos;

    // Done! no read to write data body for client
    return true;
}

} // namespace Drill
