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


#ifndef _COMMON_H_
#define _COMMON_H_

#ifdef _WIN32
// The order of inclusion is important. Including winsock2 before everything else
// ensures that the correct typedefs are defined and that the older typedefs defined
// in winsock and windows.h are not picked up.
#include <winsock2.h>
#include <windows.h>
#endif

#include <stdint.h>
#include <string>
#include <vector>
#include <boost/shared_ptr.hpp>

#define DRILL_RPC_VERSION 1

#define LENGTH_PREFIX_MAX_LENGTH 5
#define LEN_PREFIX_BUFLEN LENGTH_PREFIX_MAX_LENGTH

#define MAX_CONNECT_STR 4096
#define MAX_SOCK_RD_BUFSIZE  1024

#define MEM_CHUNK_SIZE 64*1024; // 64K
#define MAX_MEM_ALLOC_SIZE 256*1024*1024; // 256 MB

#ifdef _DEBUG
#define EXTRA_DEBUGGING
#define CODER_DEBUGGING
#endif

namespace Drill {

typedef std::vector<uint8_t> DataBuf;

typedef uint8_t Byte_t;
typedef Byte_t * ByteBuf_t;

class FieldMetadata;
typedef boost::shared_ptr< std::vector<Drill::FieldMetadata*> > FieldDefPtr;

class AllocatedBuffer;
typedef AllocatedBuffer* AllocatedBufferPtr;

typedef enum{
    QRY_SUCCESS=0,
    QRY_FAILURE=1,
    QRY_SUCCESS_WITH_INFO=2,
    QRY_NO_MORE_DATA=3,
    QRY_CANCEL=4,
    QRY_OUT_OF_BOUNDS=5,
    QRY_CLIENT_OUTOFMEM=6,
    QRY_INTERNAL_ERROR=7,
    QRY_COMM_ERROR=8
} status_t;

typedef enum{
    CONN_SUCCESS=0,
    CONN_FAILURE=1,
    CONN_HANDSHAKE_FAILED=2,
    CONN_INVALID_INPUT=3,
    CONN_ZOOKEEPER_ERROR=4
} connectionStatus_t;

typedef enum{
    LOG_TRACE=0,
    LOG_DEBUG=1,
    LOG_INFO=2,
    LOG_WARNING=3,
    LOG_ERROR=4,
    LOG_FATAL=5
} logLevel_t;

typedef enum{
    CAT_CONN=0,
    CAT_QUERY=1
} errCategory_t;

typedef enum{
    RET_SUCCESS=0,
    RET_FAILURE=1
} ret_t;

} // namespace Drill

#endif

