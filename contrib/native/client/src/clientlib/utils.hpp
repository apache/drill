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
#include <boost/thread.hpp>

#include "drill/common.hpp"
#include "drill/drillClient.hpp"

namespace Drill{

// Wrapper Class to keep track of allocated memory
class AllocatedBuffer{
    public:
    AllocatedBuffer(size_t l);
    ~AllocatedBuffer();

    ByteBuf_t m_pBuffer;
    size_t    m_bufSize;
    
    // keep track of allocated memory. The client lib blocks
    // if we have allocated up to a limit (defined in drillClientConfig).
    static boost::mutex s_memCVMutex;
    static boost::condition_variable s_memCV;
    static size_t s_allocatedMem;
    static bool s_isBufferLimitReached;

};

class Utils{
    public:
        //allocate memory for Record Batches
        static ByteBuf_t allocateBuffer(size_t len);
        static void freeBuffer(ByteBuf_t b, size_t len);
}; // Utils

} // namespace Drill

#endif
