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

#include <stdlib.h>
#include "utils.hpp"
#include "drill/common.hpp"

namespace Drill{


boost::mutex AllocatedBuffer::s_memCVMutex;
boost::condition_variable AllocatedBuffer::s_memCV;
size_t AllocatedBuffer::s_allocatedMem=0;
bool AllocatedBuffer::s_isBufferLimitReached=false;

ByteBuf_t Utils::allocateBuffer(size_t len){
    boost::lock_guard<boost::mutex> memLock(AllocatedBuffer::s_memCVMutex);
    AllocatedBuffer::s_allocatedMem+=len;
    //http://stackoverflow.com/questions/2688466/why-mallocmemset-is-slower-than-calloc
    ByteBuf_t b = (ByteBuf_t)calloc(len, sizeof(Byte_t)); 
    size_t safeSize= DrillClientConfig::getBufferLimit()-MEM_CHUNK_SIZE;
    if(b!=NULL && AllocatedBuffer::s_allocatedMem >= safeSize){
        AllocatedBuffer::s_isBufferLimitReached=true;
    }
    return b;
}

void Utils::freeBuffer(ByteBuf_t b, size_t len){ 
    boost::lock_guard<boost::mutex> memLock(AllocatedBuffer::s_memCVMutex);
    AllocatedBuffer::s_allocatedMem-=len;
    free(b); 
    size_t safeSize= DrillClientConfig::getBufferLimit()-MEM_CHUNK_SIZE;
    if(b!=NULL && AllocatedBuffer::s_allocatedMem < safeSize){
        AllocatedBuffer::s_isBufferLimitReached=false;
        //signal any waiting threads
        AllocatedBuffer::s_memCV.notify_one();
    }
}


AllocatedBuffer::AllocatedBuffer(size_t l){
    m_pBuffer=NULL;
    m_pBuffer=Utils::allocateBuffer(l);
    m_bufSize=m_pBuffer!=NULL?l:0;
}

AllocatedBuffer::~AllocatedBuffer(){
    Utils::freeBuffer(m_pBuffer, m_bufSize); 
    m_pBuffer=NULL; 
    m_bufSize=0;
}

} // namespace 
