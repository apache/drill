<#-- Licensed to the Apache Software Foundation (ASF) under one or more contributor 
  license agreements. See the NOTICE file distributed with this work for additional 
  information regarding copyright ownership. The ASF licenses this file to 
  You under the Apache License, Version 2.0 (the "License"); you may not use 
  this file except in compliance with the License. You may obtain a copy of 
  the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required 
  by applicable law or agreed to in writing, software distributed under the 
  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS 
  OF ANY KIND, either express or implied. See the License for the specific 
  language governing permissions and limitations under the License. -->

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

import com.google.common.collect.Lists;
import com.google.common.collect.ObjectArrays;
import com.google.common.base.Charsets;
import com.google.common.collect.ObjectArrays;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;

import org.apache.commons.lang3.ArrayUtils;

import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.proto.SchemaDefProtos;
import org.apache.drill.exec.proto.UserBitShared.FieldMetadata;
import org.apache.drill.exec.record.*;
import org.apache.drill.exec.vector.*;
import org.apache.drill.exec.expr.holders.*;
import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.common.types.TypeProtos.*;
import org.apache.drill.common.types.Types;

import com.sun.codemodel.JType;
import com.sun.codemodel.JCodeModel;

import java.util.Arrays;
import java.util.Random;
import java.util.List;

import java.io.Closeable;
import java.io.InputStream;
import java.io.InputStreamReader;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;






