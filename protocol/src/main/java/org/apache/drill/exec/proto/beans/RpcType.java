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

public enum RpcType implements com.dyuproject.protostuff.EnumLite<RpcType>
{
    HANDSHAKE(0),
    ACK(1),
    GOODBYE(2),
    RUN_QUERY(3),
    CANCEL_QUERY(4),
    REQUEST_RESULTS(5),
    RESUME_PAUSED_QUERY(11),
    GET_QUERY_PLAN_FRAGMENTS(12),
    GET_CATALOGS(14),
    GET_SCHEMAS(15),
    GET_TABLES(16),
    GET_COLUMNS(17),
    CREATE_PREPARED_STATEMENT(22),
    GET_SERVER_META(8),
    QUERY_DATA(6),
    QUERY_HANDLE(7),
    QUERY_PLAN_FRAGMENTS(13),
    CATALOGS(18),
    SCHEMAS(19),
    TABLES(20),
    COLUMNS(21),
    PREPARED_STATEMENT(23),
    SERVER_META(9),
    QUERY_RESULT(10),
    SASL_MESSAGE(24);
    
    public final int number;
    
    private RpcType (int number)
    {
        this.number = number;
    }
    
    public int getNumber()
    {
        return number;
    }
    
    public static RpcType valueOf(int number)
    {
        switch(number) 
        {
            case 0: return HANDSHAKE;
            case 1: return ACK;
            case 2: return GOODBYE;
            case 3: return RUN_QUERY;
            case 4: return CANCEL_QUERY;
            case 5: return REQUEST_RESULTS;
            case 6: return QUERY_DATA;
            case 7: return QUERY_HANDLE;
            case 8: return GET_SERVER_META;
            case 9: return SERVER_META;
            case 10: return QUERY_RESULT;
            case 11: return RESUME_PAUSED_QUERY;
            case 12: return GET_QUERY_PLAN_FRAGMENTS;
            case 13: return QUERY_PLAN_FRAGMENTS;
            case 14: return GET_CATALOGS;
            case 15: return GET_SCHEMAS;
            case 16: return GET_TABLES;
            case 17: return GET_COLUMNS;
            case 18: return CATALOGS;
            case 19: return SCHEMAS;
            case 20: return TABLES;
            case 21: return COLUMNS;
            case 22: return CREATE_PREPARED_STATEMENT;
            case 23: return PREPARED_STATEMENT;
            case 24: return SASL_MESSAGE;
            default: return null;
        }
    }
}
