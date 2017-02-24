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
// Generated by http://code.google.com/p/protostuff/ ... DO NOT EDIT!
// Generated from protobuf

package org.apache.drill.exec.proto.beans;

public enum OrderBySupport implements com.dyuproject.protostuff.EnumLite<OrderBySupport>
{
    OB_UNKNOWN(0),
    OB_UNRELATED(1),
    OB_EXPRESSION(2);
    
    public final int number;
    
    private OrderBySupport (int number)
    {
        this.number = number;
    }
    
    public int getNumber()
    {
        return number;
    }
    
    public static OrderBySupport valueOf(int number)
    {
        switch(number) 
        {
            case 0: return OB_UNKNOWN;
            case 1: return OB_UNRELATED;
            case 2: return OB_EXPRESSION;
            default: return null;
        }
    }
}
