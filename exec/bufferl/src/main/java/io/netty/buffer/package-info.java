/**
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



/**
* This package implements a memory manager for off-heap data based on pools and arenas.
* It is derived from the netty memory manager with the following changes:
*    - Classes have been redefined to store data little-endian.  (Append "L" to class names)
*    - A "trim" method has been added to reduce a buffer's maxCapacity and free the extra memory.
*    - allocate(min, max) 
*    
* This manager is based on the following memory abstractions:
*     - a "chunk" is a piece of memory allocated from the operating system,
*     - a "page" is is a piece of memory allocated from a chunk, and
*     - an "element" is a piece of memory allocated from a page.
*    
* This memory manager classifies the memory request according to the size of requested memory.
*    - "subpage" memory manager which breaks a page into equal sized elements.
*      It uses a bitmap to indicate which ones are free.  (why not linked list?)
*    - "normal" memory manager which breaks a chunk into runs of pages.
*       Management is done using the "buddy system", so each run is a power-of-2 pages.
*    - "huge" memory manager which allocates memory larger than a chunk.
*       It goes straight to the operating system.
*    
*  The PoolArena utilizes these three memory managers, trying to minimize fragmentation 
*  in the big buffers while keeping good performance for the small buffers.
*  
*  The three submanagers are:
*    subpage allocations.  A page is divided into equal sized elements with a bitmask indicating
*                        which elements are free. The page is placed into a pool (linked list)
*                        according to the size of the elements.
*    Normal allocations. A chunk is divided into multiple pages according to the buddy system.
*                        Chunks are kept in a list partially ordered to minimize fragmentation.
*    Huge allocations.   Memory larger than a chunk is allocated directly from the OS.
*    
*    To avoid contention, there are multiple "arenas" rather than a single monolithic memory manager.
*    Threads are assigned to an arena on a round-robin basis, and a buffer is always returned 
*    to the arena of its origin.
*                                           
*    
*        Note for "normal" allocations: Rather than keeping one fully ordered list of chunks, 
*        it keeps several lists, and always
*        searches the first list, followed by the next list. Thus it achieves an approximate ordering without
*        having to do a full sort.
*             
*    
* This PoolArena allocator is configured according to Java environment variables.
*    io.netty.allocator.numDirectArenas - the number of arenas to divide among threads.
*    io.netty.allocator.pageSize - The size of each page, must be power of 2.
*    io.netty.allocator.maxOrder - Specifies chunkSize as pageSize * 2^maxOrder.
* 
* It may not make sense to use this allocator for small allocations.
*     Each buffer which is allocated uses a Java class which must also be allocated.
*     Thus we may not save on Java memory management for small buffers.
*/

package io.netty.buffer;