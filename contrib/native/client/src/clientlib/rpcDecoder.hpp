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


#ifndef RPC_DECODER_H
#define RPC_DECODER_H

#include "rpcMessage.hpp"

namespace Drill {

class RpcDecoder {
    public:
        RpcDecoder() { }
        ~RpcDecoder() { }
        // bool Decode(const DataBuf& buf);
        // bool Decode(const DataBuf& buf, InBoundRpcMessage& msg);
        static int LengthDecode(const uint8_t* buf, uint32_t* length); // read the length prefix (at most 4 bytes)
        static int Decode(const uint8_t* buf, int length, InBoundRpcMessage& msg);
};

} // namespace Drill
#endif
