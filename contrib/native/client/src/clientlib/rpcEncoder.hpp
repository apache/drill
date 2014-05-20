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


#ifndef RPC_ENCODER_H
#define RPC_ENCODER_H

#include "rpcMessage.hpp"

namespace Drill {

class RpcEncoder {
    public:
        RpcEncoder() {}
        ~RpcEncoder() { }
        bool Encode(DataBuf& buf,OutBoundRpcMessage& msg);
        static const uint32_t HEADER_TAG;
        static const uint32_t PROTOBUF_BODY_TAG;
        static const uint32_t RAW_BODY_TAG;
        static const uint32_t HEADER_TAG_LENGTH;
        static const uint32_t PROTOBUF_BODY_TAG_LENGTH;
        static const uint32_t RAW_BODY_TAG_LENGTH;
};

// copy from java code
inline int getRawVarintSize(uint32_t value) {
    int count = 0;
    while (true) {
        if ((value & ~0x7F) == 0) {
            count++;
            return count;
        } else {
            count++;
            value >>= 7;
        }
    }
}

} // namespace Drill
#endif
