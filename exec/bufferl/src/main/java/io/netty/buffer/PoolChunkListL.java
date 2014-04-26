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
package io.netty.buffer;

import io.netty.util.internal.StringUtil;

final class PoolChunkListL<T> {
    private final PoolArenaL<T> arena;
    private final PoolChunkListL<T> nextList;
    PoolChunkListL<T> prevList;

    private final int minUsage;
    private final int maxUsage;

    private PoolChunkL<T> head;

    // TODO: Test if adding padding helps under contention
    //private long pad0, pad1, pad2, pad3, pad4, pad5, pad6, pad7;

    PoolChunkListL(PoolArenaL<T> arena, PoolChunkListL<T> nextList, int minUsage, int maxUsage) {
        this.arena = arena;
        this.nextList = nextList;
        this.minUsage = minUsage;
        this.maxUsage = maxUsage;
    }

    boolean allocate(PooledByteBufL<T> buf, int reqCapacity, int normCapacity) {
        if (head == null) {
            return false;
        }

        for (PoolChunkL<T> cur = head;;) {
            long handle = cur.allocate(normCapacity);
            if (handle < 0) {
                cur = cur.next;
                if (cur == null) {
                    return false;
                }
            } else {
                cur.initBuf(buf, handle, reqCapacity);
                if (cur.usage() >= maxUsage) {
                    remove(cur);
                    nextList.add(cur);
                }
                return true;
            }
        }
    }

    void free(PoolChunkL<T> chunk, long handle) {
        chunk.free(handle);
        if (chunk.usage() < minUsage) {
            remove(chunk);
            if (prevList == null) {
                assert chunk.usage() == 0;
                arena.destroyChunk(chunk);
            } else {
                prevList.add(chunk);
            }
        }
    }

    void add(PoolChunkL<T> chunk) {
        if (chunk.usage() >= maxUsage) {
            nextList.add(chunk);
            return;
        }

        chunk.parent = this;
        if (head == null) {
            head = chunk;
            chunk.prev = null;
            chunk.next = null;
        } else {
            chunk.prev = null;
            chunk.next = head;
            head.prev = chunk;
            head = chunk;
        }
    }

    private void remove(PoolChunkL<T> cur) {
        if (cur == head) {
            head = cur.next;
            if (head != null) {
                head.prev = null;
            }
        } else {
            PoolChunkL<T> next = cur.next;
            cur.prev.next = next;
            if (next != null) {
                next.prev = cur.prev;
            }
        }
    }

    @Override
    public String toString() {
        if (head == null) {
            return "none";
        }

        StringBuilder buf = new StringBuilder();
        for (PoolChunkL<T> cur = head;;) {
            buf.append(cur);
            cur = cur.next;
            if (cur == null) {
                break;
            }
            buf.append(StringUtil.NEWLINE);
        }

        return buf.toString();
    }
}
