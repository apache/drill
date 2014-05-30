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

#ifndef __UTILS_H
#define __UTILS_H

#include <sstream>
#include <ostream>
#include <fstream>
#include <string>
#include <stdlib.h>

#include "drill/common.hpp"

namespace Drill{

class Utils{
    public:

        //allocate memory for Record Batches
        static ByteBuf_t allocateBuffer(size_t len){
            //http://stackoverflow.com/questions/2688466/why-mallocmemset-is-slower-than-calloc
            ByteBuf_t b = (ByteBuf_t)calloc(len, sizeof(Byte_t)); return b;
        }
        static void freeBuffer(ByteBuf_t b){ free(b); }

}; // Utils


} // namespace Drill

#endif
