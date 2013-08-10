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

import org.apache.drill.exec.compile.sig.CodeGeneratorSignature;
import org.apache.drill.exec.compile.sig.DefaultGeneratorSignature;
import org.apache.drill.exec.compile.sig.SignatureHolder;

public class TemplateClassDefinition<T>{
  
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TemplateClassDefinition.class);
  
  private final Class<T> externalInterface;
  private final String templateClassName;
  private final Class<?> internalInterface;
  private final SignatureHolder signature;
  
  public TemplateClassDefinition(Class<T> externalInterface, String templateClassName, Class<?> internalInterface) {
    this(externalInterface, templateClassName, internalInterface, DefaultGeneratorSignature.class);
  }
  
  public <X extends CodeGeneratorSignature> TemplateClassDefinition(Class<T> externalInterface, String templateClassName, Class<?> internalInterface, Class<X> signature) {
    super();
    this.externalInterface = externalInterface;
    this.templateClassName = templateClassName;
    this.internalInterface = internalInterface;
    SignatureHolder holder = null;
    try{
      holder = new SignatureHolder(signature);
    }catch(Exception ex){
      logger.error("Failure while trying to build signature holder for signature. {}", signature.getSimpleName(), ex);
    }
    this.signature = holder;

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

  public SignatureHolder getSignature(){
    return signature;
  }

  @Override
  public String toString() {
    return "TemplateClassDefinition [externalInterface=" + externalInterface + ", templateClassName="
        + templateClassName + ", internalInterface=" + internalInterface + ", signature=" + signature + "]";
  }
  
  
}
