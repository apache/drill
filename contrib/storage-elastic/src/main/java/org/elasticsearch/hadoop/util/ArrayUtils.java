/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.hadoop.util;

import org.elasticsearch.hadoop.EsHadoopIllegalArgumentException;

// taken from org.apache.lucene.util
abstract class ArrayUtils {

    static byte[] grow(byte[] array, int minSize) {
        assert minSize >= 0 : "size must be positive (got " + minSize + "): likely integer overflow?";
        if (array.length < minSize) {
            byte[] newArray = new byte[oversize(minSize, 1)];
            System.arraycopy(array, 0, newArray, 0, array.length);
            return newArray;
        }
        else
            return array;
    }

    static int oversize(int minTargetSize, int bytesPerElement) {

        if (minTargetSize < 0) {
            // catch usage that accidentally overflows int
            throw new EsHadoopIllegalArgumentException("invalid array size " + minTargetSize);
        }

        if (minTargetSize == 0) {
            // wait until at least one element is requested
            return 0;
        }

        // asymptotic exponential growth by 1/8th, favors
        // spending a bit more CPU to not tie up too much wasted
        // RAM:
        int extra = minTargetSize >> 3;

        if (extra < 3) {
            // for very small arrays, where constant overhead of
            // realloc is presumably relatively high, we grow
            // faster
            extra = 3;
        }

        int newSize = minTargetSize + extra;

        // add 7 to allow for worst case byte alignment addition below:
        if (newSize + 7 < 0) {
            // int overflowed -- return max allowed array size
            return Integer.MAX_VALUE;
        }

        if (Constants.JRE_IS_64BIT) {
            // round up to 8 byte alignment in 64bit env
            switch (bytesPerElement) {
            case 4:
                // round up to multiple of 2
                return (newSize + 1) & 0x7ffffffe;
            case 2:
                // round up to multiple of 4
                return (newSize + 3) & 0x7ffffffc;
            case 1:
                // round up to multiple of 8
                return (newSize + 7) & 0x7ffffff8;
            case 8:
                // no rounding
            default:
                // odd (invalid?) size
                return newSize;
            }
        }
        else {
            // round up to 4 byte alignment in 64bit env
            switch (bytesPerElement) {
            case 2:
                // round up to multiple of 2
                return (newSize + 1) & 0x7ffffffe;
            case 1:
                // round up to multiple of 4
                return (newSize + 3) & 0x7ffffffc;
            case 4:
            case 8:
                // no rounding
            default:
                // odd (invalid?) size
                return newSize;
            }
        }
    }
}