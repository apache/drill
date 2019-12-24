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
package org.elasticsearch.hadoop.serialization.json;

import java.lang.ref.SoftReference;

import org.codehaus.jackson.util.BufferRecycler;
import org.codehaus.jackson.util.TextBuffer;

/**
 * Backported class from Jackson 1.8.8 for Jackson 1.5.2
 *
 * Helper class used for efficient encoding of JSON String values (including
 * JSON field names) into Strings or UTF-8 byte arrays.
 *<p>
 * Note that methods in here are somewhat optimized, but not ridiculously so.
 * Reason is that conversion method results are expected to be cached so that
 * these methods will not be hot spots during normal operation.
 *
 * @since 1.6
 */
public class BackportedJsonStringEncoder {

    private final static char[] HEX_CHARS_SOURCE = "0123456789ABCDEF".toCharArray();
    private final static char[] HEX_CHARS = HEX_CHARS_SOURCE.clone();

    /**
     * Lookup table used for determining which output characters in
     * 7-bit ASCII range need to be quoted.
     */
    final static int[] sOutputEscapes128;
    static {
        int[] table = new int[128];
        // Control chars need generic escape sequence
        for (int i = 0; i < 32; ++i) {
            // 04-Mar-2011, tatu: Used to use "-(i + 1)", replaced with constant
            table[i] = -1;
        }
        /* Others (and some within that range too) have explicit shorter
         * sequences
         */
        table['"'] = '"';
        table['\\'] = '\\';
        // Escaping of slash is optional, so let's not add it
        table[0x08] = 'b';
        table[0x09] = 't';
        table[0x0C] = 'f';
        table[0x0A] = 'n';
        table[0x0D] = 'r';
        sOutputEscapes128 = table;
    }

    /**
     * This <code>ThreadLocal</code> contains a {@link java.lang.ref.SoftRerefence}
     * to a {@link BufferRecycler} used to provide a low-cost
     * buffer recycling between reader and writer instances.
     */
    final protected static ThreadLocal<SoftReference<BackportedJsonStringEncoder>> _threadEncoder = new ThreadLocal<SoftReference<BackportedJsonStringEncoder>>();

    /**
     * Lazily constructed text buffer used to produce JSON encoded Strings
     * as characters (without UTF-8 encoding)
     */
    protected TextBuffer _textBuffer;

    /**
     * Temporary buffer used for composing quote/escape sequences
     */
    protected final char[] _quoteBuffer;


    public BackportedJsonStringEncoder() {
        _quoteBuffer = new char[6];
        _quoteBuffer[0] = '\\';
        _quoteBuffer[2] = '0';
        _quoteBuffer[3] = '0';
    }

    /**
     * Factory method for getting an instance; this is either recycled per-thread instance,
     * or a newly constructed one.
     */
    public static BackportedJsonStringEncoder getInstance() {
        SoftReference<BackportedJsonStringEncoder> ref = _threadEncoder.get();
        BackportedJsonStringEncoder enc = (ref == null) ? null : ref.get();

        if (enc == null) {
            enc = new BackportedJsonStringEncoder();
            _threadEncoder.set(new SoftReference<BackportedJsonStringEncoder>(enc));
        }
        return enc;
    }


    /**
     * Method that will quote text contents using JSON standard quoting,
     * and return results as a character array
     */
    public char[] quoteAsString(String input) {
        TextBuffer textBuffer = _textBuffer;
        if (textBuffer == null) {
            // no allocator; can add if we must, shouldn't need to
            _textBuffer = textBuffer = new TextBuffer(null);
        }
        char[] outputBuffer = textBuffer.emptyAndGetCurrentSegment();
        final int[] escCodes = sOutputEscapes128;
        final int escCodeCount = escCodes.length;
        int inPtr = 0;
        final int inputLen = input.length();
        int outPtr = 0;

        outer_loop: while (inPtr < inputLen) {
            tight_loop: while (true) {
                char c = input.charAt(inPtr);
                if (c < escCodeCount && escCodes[c] != 0) {
                    break tight_loop;
                }
                if (outPtr >= outputBuffer.length) {
                    outputBuffer = textBuffer.finishCurrentSegment();
                    outPtr = 0;
                }
                outputBuffer[outPtr++] = c;
                if (++inPtr >= inputLen) {
                    break outer_loop;
                }
            }
        // something to escape; 2 or 6-char variant?
        int escCode = escCodes[input.charAt(inPtr++)];
        int length = _appendSingleEscape(escCode, _quoteBuffer);
        if ((outPtr + length) > outputBuffer.length) {
            int first = outputBuffer.length - outPtr;
            if (first > 0) {
                System.arraycopy(_quoteBuffer, 0, outputBuffer, outPtr, first);
            }
            outputBuffer = textBuffer.finishCurrentSegment();
            int second = length - first;
            System.arraycopy(_quoteBuffer, first, outputBuffer, outPtr, second);
            outPtr += second;
        }
        else {
            System.arraycopy(_quoteBuffer, 0, outputBuffer, outPtr, length);
            outPtr += length;
        }

        }
        textBuffer.setCurrentLength(outPtr);
        return textBuffer.contentsAsArray();
    }

    private int _appendSingleEscape(int escCode, char[] quoteBuffer) {
        if (escCode < 0) { // control char, value -(char + 1)
            int value = -(escCode + 1);
            quoteBuffer[1] = 'u';
            // We know it's a control char, so only the last 2 chars are non-0
            quoteBuffer[4] = HEX_CHARS[value >> 4];
            quoteBuffer[5] = HEX_CHARS[value & 0xF];
            return 6;
        }
        quoteBuffer[1] = (char) escCode;
        return 2;
    }
}