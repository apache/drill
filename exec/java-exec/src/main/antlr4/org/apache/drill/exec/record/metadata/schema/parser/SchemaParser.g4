parser grammar SchemaParser;

options {
  language=Java;
  tokenVocab=SchemaLexer;
}

@header {
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
}

schema: (columns | LEFT_PAREN columns RIGHT_PAREN) EOF;

columns: column (COMMA column)*;

column: (primitive_column | map_column | simple_array_column | complex_array_column);

primitive_column: column_id simple_type nullability?;

simple_array_column: column_id simple_array_type nullability?;

map_column: column_id map_type nullability?;

complex_array_column: column_id complex_array_type nullability?;

column_id
: ID # id
| QUOTED_ID # quoted_id
;

simple_type
: (INT | INTEGER) # int
| BIGINT # bigint
| FLOAT # float
| DOUBLE # double
| (DEC | DECIMAL | NUMERIC) (LEFT_PAREN NUMBER (COMMA NUMBER)? RIGHT_PAREN)? # decimal
| BOOLEAN # boolean
| (CHAR | VARCHAR | CHARACTER VARYING?) (LEFT_PAREN NUMBER RIGHT_PAREN)? # varchar
| (BINARY | VARBINARY) (LEFT_PAREN NUMBER RIGHT_PAREN)? # binary
| TIME (LEFT_PAREN NUMBER RIGHT_PAREN)? # time
| DATE # date
| TIMESTAMP (LEFT_PAREN NUMBER RIGHT_PAREN)? # timestamp
| INTERVAL (YEAR | MONTH) # interval_year
| INTERVAL (DAY | HOUR | MINUTE | SECOND)  # interval_day
| INTERVAL # interval
;

complex_type: (simple_array_type | complex_array_type);

simple_array_type: ARRAY LEFT_ANGLE_BRACKET (simple_type | map_type) RIGHT_ANGLE_BRACKET;

complex_array_type: ARRAY LEFT_ANGLE_BRACKET complex_type RIGHT_ANGLE_BRACKET;

map_type: MAP LEFT_ANGLE_BRACKET columns RIGHT_ANGLE_BRACKET;

nullability: NOT NULL;
