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
// Generated by http://code.google.com/p/protostuff/ ... DO NOT EDIT!
// Generated from protobuf

package org.apache.drill.exec.proto.beans;

public enum CorrelationNamesSupport implements com.dyuproject.protostuff.EnumLite<CorrelationNamesSupport>
{
    CN_NONE(1),
    CN_DIFFERENT_NAMES(2),
    CN_ANY(3);
    
    public final int number;
    
    private CorrelationNamesSupport (int number)
    {
        this.number = number;
    }
    
    public int getNumber()
    {
        return number;
    }
    
    public static CorrelationNamesSupport valueOf(int number)
    {
        switch(number) 
        {
            case 1: return CN_NONE;
            case 2: return CN_DIFFERENT_NAMES;
            case 3: return CN_ANY;
            default: return null;
        }
    }
}
