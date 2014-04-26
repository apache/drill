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
package org.apache.drill.exec.expr.fn;

import java.util.HashSet;
import java.util.Set;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.expression.FunctionCall;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.Types;
import org.apache.drill.common.util.PathScanner;
import org.apache.drill.exec.expr.fn.impl.hive.ObjectInspectorHelper;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFBridge;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

import com.google.common.collect.ArrayListMultimap;

public class HiveFunctionImplementationRegistry {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(HiveFunctionImplementationRegistry.class);

  private ArrayListMultimap<String, Class<? extends GenericUDF>> methodsGenericUDF = ArrayListMultimap.create();
  private ArrayListMultimap<String, Class<? extends UDF>> methodsUDF = ArrayListMultimap.create();
  private HashSet<Class<?>> nonDeterministicUDFs = new HashSet<>();

  /**
   * Scan the classpath for implementation of GenericUDF/UDF interfaces,
   * extracts function annotation and store the
   * (function name) --> (implementation class) mappings.
   * @param config
   */
  public HiveFunctionImplementationRegistry(DrillConfig config){
    Set<Class<? extends GenericUDF>> genericUDFClasses = PathScanner.scanForImplementations(GenericUDF.class, null);
    for (Class<? extends GenericUDF> clazz : genericUDFClasses)
      register(clazz, methodsGenericUDF);

    Set<Class<? extends UDF>> udfClasses = PathScanner.scanForImplementations(UDF.class, null);
    for (Class<? extends UDF> clazz : udfClasses)
      register(clazz, methodsUDF);
  }

  private <C,I> void register(Class<? extends I> clazz, ArrayListMultimap<String,Class<? extends I>> methods) {
    Description desc = clazz.getAnnotation(Description.class);
    String[] names;
    if(desc != null){
      names = desc.name().split(",");
      for(int i=0; i<names.length; i++) names[i] = names[i].trim();
    }else{
      names = new String[]{clazz.getName().replace('.', '_')};
    }
    
    UDFType type = clazz.getAnnotation(UDFType.class);
    if (type != null && type.deterministic()) nonDeterministicUDFs.add(clazz);


    for(int i=0; i<names.length;i++){
      methods.put(names[i], clazz);
    }
  }

  /**
   * Find the UDF class for given function name and check if it accepts the given input argument
   * types. If a match is found, create a holder and return
   * @param call
   * @return
   */
  public HiveFuncHolder getFunction(FunctionCall call){
    HiveFuncHolder holder;
    MajorType[] argTypes = new MajorType[call.args.size()];
    ObjectInspector[] argOIs = new ObjectInspector[call.args.size()];
    for(int i=0; i<call.args.size(); i++) {
      argTypes[i] = call.args.get(i).getMajorType();
      argOIs[i] = ObjectInspectorHelper.getDrillObjectInspector(argTypes[i].getMinorType());
    }

    // search in GenericUDF list
    for(Class<? extends GenericUDF> clazz: methodsGenericUDF.get(call.getName())) {
      holder = matchAndCreateGenericUDFHolder(clazz, argTypes, argOIs);
      if(holder != null)
        return holder;
    }

    // search in UDF list
    for (Class<? extends UDF> clazz : methodsUDF.get(call.getName())) {
      holder = matchAndCreateUDFHolder(call.getName(), clazz, argTypes, argOIs);
      if (holder != null)
        return holder;
    }

    return null;
  }

  private HiveFuncHolder matchAndCreateGenericUDFHolder(Class<? extends GenericUDF> udfClazz,
                                              MajorType[] argTypes,
                                              ObjectInspector[] argOIs) {
    // probe UDF to find if the arg types and acceptable
    // if acceptable create a holder object
    try {
      GenericUDF udfInstance = udfClazz.newInstance();
      ObjectInspector returnOI = udfInstance.initialize(argOIs);
      return new HiveFuncHolder(
        udfClazz,
        argTypes,
        returnOI,
        Types.optional(ObjectInspectorHelper.getDrillType(returnOI)),
        nonDeterministicUDFs.contains(udfClazz));
    } catch(IllegalAccessException | InstantiationException e) {
      logger.debug("Failed to instantiate class", e);
    } catch(UDFArgumentException e) { /*ignore this*/ }

    return null;
  }

  private HiveFuncHolder matchAndCreateUDFHolder(String udfName,
                                                 Class<? extends UDF> udfClazz,
                                                 MajorType[] argTypes,
                                                 ObjectInspector[] argOIs) {
    try {
      GenericUDF udfInstance = new GenericUDFBridge(udfName, false/* is operator */, udfClazz);
      ObjectInspector returnOI = udfInstance.initialize(argOIs);

      return new HiveFuncHolder(
        udfName,
        udfClazz,
        argTypes,
        returnOI,
        Types.optional(ObjectInspectorHelper.getDrillType(returnOI)),
        nonDeterministicUDFs.contains(udfClazz));
    } catch(UDFArgumentException e) { /*ignore this*/ }

    return null;
  }
}
