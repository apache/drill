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
#include <set>
#include <string>
#include <vector>
#include <boost/shared_ptr.hpp>

#define DRILL_RPC_VERSION 5

#define LENGTH_PREFIX_MAX_LENGTH 5
#define LEN_PREFIX_BUFLEN LENGTH_PREFIX_MAX_LENGTH

#define MAX_CONNECT_STR 4096
#define MAX_SOCK_RD_BUFSIZE  1024

#define MEM_CHUNK_SIZE 64*1024; // 64K
#define MAX_MEM_ALLOC_SIZE 256*1024*1024; // 256 MB

#define MAX_BATCH_SIZE  65536; // see RecordBatch.java 
#define ENABLE_CONNECTION_POOL_ENV  "DRILL_ENABLE_CONN_POOL"
#define DEFAULT_MAX_CONCURRENT_CONNECTIONS 10
#define MAX_CONCURRENT_CONNECTIONS_ENV  "DRILL_MAX_CONN"

#ifdef _DEBUG
#define EXTRA_DEBUGGING
#define CODER_DEBUGGING
#endif

// http://www.boost.org/doc/libs/1_54_0/doc/html/boost_asio/reference/basic_stream_socket/cancel/overload1.html
// : "Calls to cancel() will always fail with boost::asio::error::operation_not_supported when run on Windows XP, Windows Server 2003, and earlier versions of Windows..."
// As such, achieving cancel needs to be implemented differently;
#if defined(_WIN32)  && !defined(_WIN64)
#define WIN32_SHUTDOWN_ON_TIMEOUT
#endif // _WIN32 && !_WIN64


//DEPRECATED MACRO
#if defined(__GNUC__) || defined(__llvm__)
#define DEPRECATED __attribute__((deprecated))
#elif defined(_MSC_VER)
#define DEPRECATED __declspec(deprecated)
#else
#pragma message("WARNING: DEPRECATED not available for this compiler")
#define DEPRECATED
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
    QRY_COMM_ERROR=8,
    QRY_PENDING = 9,
    QRY_RUNNING = 10,
    QRY_COMPLETED = 11,
    QRY_CANCELED = 12,
    QRY_FAILED = 13,
    QRY_UNKNOWN_QUERY = 14,
    QRY_TIMEOUT = 15
} status_t;

typedef enum{
    CONN_SUCCESS=0,
    CONN_FAILURE=1,
    CONN_HANDSHAKE_FAILED=2,
    CONN_INVALID_INPUT=3,
    CONN_ZOOKEEPER_ERROR=4,
    CONN_HANDSHAKE_TIMEOUT=5,
    CONN_HOSTNAME_RESOLUTION_ERROR=6,
    CONN_AUTH_FAILED=7,
    CONN_BAD_RPC_VER=8,
    CONN_DEAD=9,
    CONN_NOTCONNECTED=10,
    CONN_ALREADYCONNECTED=11
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


// User Property Names
#define USERPROP_USERNAME "userName"
#define USERPROP_PASSWORD "password"
#define USERPROP_SCHEMA   "schema"
#define USERPROP_USESSL   "useSSL"        // Not implemented yet
#define USERPROP_FILEPATH "pemLocation"   // Not implemented yet
#define USERPROP_FILENAME "pemFile"       // Not implemented yet
#define USERPROP_IMPERSONATION_TARGET "impersonation_target"

// Bitflags to describe user properties
// Used in DrillUserProperties::USER_PROPERTIES
#define USERPROP_FLAGS_SERVERPROP 0x00000001
#define USERPROP_FLAGS_SSLPROP    0x00000002
#define USERPROP_FLAGS_USERNAME   0x00000004
#define USERPROP_FLAGS_PASSWORD   0x00000008
#define USERPROP_FLAGS_FILENAME   0x00000010
#define USERPROP_FLAGS_FILEPATH   0x00000020
#define USERPROP_FLAGS_STRING     0x00000040
#define USERPROP_FLAGS_BOOLEAN    0x00000080

#define IS_BITSET(val, bit) \
    ((val&bit)==bit)

} // namespace Drill

#endif

