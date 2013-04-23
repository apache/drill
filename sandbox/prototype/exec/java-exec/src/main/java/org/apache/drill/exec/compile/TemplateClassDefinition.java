/*******************************************************************************
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
 ******************************************************************************/
package org.apache.drill.exec.compile;

import java.lang.reflect.Method;


public class TemplateClassDefinition<T, I>{
  
  private final Class<T> externalInterface;
  private final String templateClassName;
  private final Class<?> internalInterface;
  private final Class<I> constructorObject;
  
  public TemplateClassDefinition(Class<T> externalInterface, String templateClassName, Class<?> internalInterface, Class<I> constructorObject) {
    super();
    this.externalInterface = externalInterface;
    this.templateClassName = templateClassName; 
    this.internalInterface = internalInterface;
    this.constructorObject = constructorObject;
  }

  public Class<T> getExternalInterface() {
    return externalInterface;
  }

  
  public Class<?> getInternalInterface() {
    return internalInterface;
  }

  public String getTemplateClassName() {
    return templateClassName;
  }

  public Class<I> getConstructorObject() {
    return constructorObject;
  }
  
  
  
  
}
