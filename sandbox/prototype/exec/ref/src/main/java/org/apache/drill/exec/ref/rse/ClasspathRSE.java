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
package org.apache.drill.exec.ref.rse;

import java.io.IOException;
import java.net.URL;
import java.util.Collection;
import java.util.Collections;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.logical.StorageEngineConfigBase;
import org.apache.drill.common.logical.data.Scan;
import org.apache.drill.exec.ref.exceptions.SetupException;
import org.apache.drill.exec.ref.rops.DataWriter.ConverterType;
import org.apache.drill.exec.ref.rops.ROP;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

public class ClasspathRSE extends RSEBase {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ClasspathRSE.class);

  private DrillConfig dConfig;
  private SchemaPath rootPath;
  
  public ClasspathRSE(ClasspathRSEConfig engineConfig, DrillConfig dConfig) throws SetupException{
    this.dConfig = dConfig;
  }

  
  @JsonTypeName("classpath")
  public static class ClasspathRSEConfig extends StorageEngineConfigBase {
    @JsonCreator
    public ClasspathRSEConfig(@JsonProperty("name") String name) {
      super(name);
    }
  }
  
  public static class ClasspathInputConfig implements ReadEntry{
    public String path;
    public ConverterType type;
    @JsonIgnore public SchemaPath rootPath; 
  }

  public boolean supportsRead() {
    return true;
  }

  @Override
  public Collection<ReadEntry> getReadEntries(Scan scan) throws IOException {
    ClasspathInputConfig c = scan.getSelection().getWith(ClasspathInputConfig.class);
    c.rootPath = scan.getOutputReference();
    return Collections.singleton((ReadEntry) c);
  }

  @Override
  public RecordReader getReader(ReadEntry readEntry, ROP parentROP) throws IOException {
    ClasspathInputConfig e = getReadEntry(ClasspathInputConfig.class, readEntry);
    URL u = RecordReader.class.getResource(e.path);
    if(u == null){
      throw new IOException(String.format("Failure finding classpath resource %s.", e.path));
    }
    switch(e.type){
    case JSON:
      return new JSONRecordReader(e.rootPath, dConfig, u.openStream(), parentROP);
    default:
      throw new UnsupportedOperationException();
    }
  }
  
  

  
}
