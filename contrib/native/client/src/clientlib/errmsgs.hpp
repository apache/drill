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

#ifndef ERRMSGS_H
#define ERRMSGS_H

#include <stdint.h>
#include <string>

namespace Drill{

#define ERR_CATEGORY_CONN 10000
#define ERR_CATEGORY_QRY 20000

#define ERR_NONE 0

#define ERR_CONN_FAILURE        1
#define ERR_CONN_EXCEPT         2
#define ERR_CONN_UNKPROTO       3
#define ERR_CONN_ZOOKEEPER      4
#define ERR_CONN_NOHSHAKE       5
#define ERR_CONN_ZKFAIL         6
#define ERR_CONN_ZKTIMOUT       7
#define ERR_CONN_ZKERR          8
#define ERR_CONN_ZKDBITERR      9
#define ERR_CONN_ZKNODBIT       10
#define ERR_CONN_ZKNOAUTH       11
#define ERR_CONN_ZKEXP          12
#define ERR_CONN_MAX            12

#define ERR_QRY_OUTOFMEM    ERR_CONN_MAX+1 
#define ERR_QRY_COMMERR     ERR_CONN_MAX+2
#define ERR_QRY_INVREADLEN  ERR_CONN_MAX+3
#define ERR_QRY_INVQUERYID  ERR_CONN_MAX+4
#define ERR_QRY_INVRPCTYPE  ERR_CONN_MAX+5
#define ERR_QRY_OUTOFORDER  ERR_CONN_MAX+6
#define ERR_QRY_INVRPC      ERR_CONN_MAX+7
#define ERR_QRY_8           ERR_CONN_MAX+8
#define ERR_QRY_FAILURE     ERR_CONN_MAX+9
#define ERR_QRY_SELVEC2     ERR_CONN_MAX+10
#define ERR_QRY_RESPFAIL    ERR_CONN_MAX+11 
#define ERR_QRY_12          ERR_CONN_MAX+12
#define ERR_QRY_UNKQRY      ERR_CONN_MAX+13
#define ERR_QRY_CANCELED    ERR_CONN_MAX+14
#define ERR_QRY_15          ERR_CONN_MAX+15
#define ERR_QRY_16          ERR_CONN_MAX+16
#define ERR_QRY_17          ERR_CONN_MAX+17
#define ERR_QRY_18          ERR_CONN_MAX+18
#define ERR_QRY_19          ERR_CONN_MAX+19
#define ERR_QRY_20          ERR_CONN_MAX+20
#define ERR_QRY_MAX         ERR_QRY_20 

    // Use only Plain Old Data types in this struc. We will declare
    // a global.
    struct ErrorMessages{
        uint32_t msgId;
        uint32_t category;
        uint32_t nArgs;
        char msgFormatStr[2048+1];
    };

    //declared in errmsgs.cpp
    //static ErrorMessages errorMessages[];


    std::string getMessage(uint32_t msgId, ...);

} // namespace Drill



#endif
