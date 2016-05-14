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
package org.apache.drill.exec.expr.fn.impl;

import java.util.regex.Matcher;

import io.netty.buffer.DrillBuf;

/**
 * A CharSequence is a readable sequence of char values. This interface provides
 * uniform, read-only access to many different kinds of char sequences. A char
 * value represents a character in the Basic Multilingual Plane (BMP) or a
 * surrogate. Refer to Unicode Character Representation for details.<br>
 * Specifically this implementation of the CharSequence adapts a Drill
 * {@link DrillBuf} to the CharSequence. The implementation is meant to be
 * re-used that is allocated once and then passed DrillBuf to adapt. This can be
 * handy to exploit API that consume CharSequence avoiding the need to create
 * string objects.
 *
 */
public class CharSequenceWrapper implements CharSequence {

    // The adapted drill buffer
    private DrillBuf buffer;
    // The start offset into the drill buffer
    private int start;
    // The end offset into the drill buffer
    private int end;

    @Override
    public int length() {
        return end - start;
    }

    @Override
    public char charAt(int index) {
        int charPos = start + index;
        if(charPos < start || charPos >= end)
        {
            throw new IndexOutOfBoundsException();
        }
        // Get the char within the DrillBuf.
        return (char) buffer.getByte(charPos);
    }

    /**
     * When using the Java regex {@link Matcher} the subSequence is only called
     * when capturing groups. Drill does not currently use capture groups in the
     * UDF so this method is not required.<br>
     * It could be implemented by creating a new CharSequenceWrapper however
     * this would imply newly allocated objects which is what this wrapper tries
     * to avoid.
     *
     */
    @Override
    public CharSequence subSequence(int start, int end) {
        throw new UnsupportedOperationException("Not implemented.");
    }

    /**
     * Set the DrillBuf to adapt to a CharSequence. This method can be used to
     * replace any previous DrillBuf thus avoiding recreating the
     * CharSequenceWrapper and thus re-using the CharSequenceWrapper object.
     *
     * @param start
     * @param end
     * @param buffer
     */
    public void setBuffer(int start, int end, DrillBuf buffer) {
        this.start = start;
        this.end = end;
        this.buffer = buffer;
    }

    /**
     * In cases where a method like {@link Matcher#replaceAll(String)} is used this
     * {@link CharSequence#toString()} method will be called, so we need to return a
     * string representing the value of this CharSequence. Note however that the
     * regexp_replace function is implemented in a way to avoid the call to toString()
     * not to uselessly create a string object.
     */
    @Override
    public String toString() {
        return StringFunctionHelpers.toStringFromUTF8(start, end, buffer);
    }

}
