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
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.logical.StorageEngineConfigBase;
import org.apache.drill.common.logical.data.Scan;
import org.apache.drill.common.logical.data.Store;
import org.apache.drill.exec.ref.exceptions.SetupException;
import org.apache.drill.exec.ref.rops.DataWriter.ConverterType;
import org.apache.drill.exec.ref.rops.ROP;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

public class FileSystemRSE extends RSEBase {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FileSystemRSE.class);

  private FileSystem fs;
  private Path basePath;
  private DrillConfig dConfig;

  public FileSystemRSE(FileSystemRSEConfig engineConfig, DrillConfig dConfig) throws SetupException{
    this.dConfig = dConfig;
    
    try {
      URI u = new URI(engineConfig.root);
      String path = u.getPath();
      
      if(path.charAt(path.length()-1) != '/') throw new SetupException(String.format("The file root provided of %s included a file '%s'.  This must be a base path.", engineConfig.root, u.getPath()));
      fs = FileSystem.get(u, new Configuration());
      basePath = new Path(u.getPath());
    } catch (URISyntaxException | IOException e) {
      throw new SetupException("Failure while reading setting up file system root path.", e);
    }
  }

  
  @JsonTypeName("fs")
  public static class FileSystemRSEConfig extends StorageEngineConfigBase {
    private String root;
    @JsonCreator
    public FileSystemRSEConfig(@JsonProperty("name") String name, @JsonProperty("root") String root) {
      super(name);
      this.root = root;
    }
  }
  
  public static class FileSystemInputConfig {
    public FileSpec[] files;
  }
  
  public static class FileSpec{
    public String path;
    public ConverterType type;
  }
  
  
  public class FSEntry implements ReadEntry{
    Path path;
    ConverterType type;
    SchemaPath rootPath;

    public FSEntry(FileSpec spec, SchemaPath rootPath){
      this.path = new Path(basePath, spec.path);
      this.type = spec.type;
      this.rootPath = rootPath;
    }
        
  }

  public class FileSystemOutputConfig {
    public String file;
    public ConverterType type;
  }

  public boolean supportsRead() {
    return true;
  }
  
  public boolean supportsWrite() {
    return true;
  }

  @Override
  public RecordRecorder getWriter(Store store) throws IOException {
    FileSystemOutputConfig config = store.getTarget().getWith(FileSystemOutputConfig.class);
    OutputStream out = fs.create(new Path(basePath, config.file));
    return new OutputStreamWriter(out, config.type, true);
  }

  @Override
  public Collection<ReadEntry> getReadEntries(Scan scan) throws IOException {
    Set<ReadEntry> s = new HashSet<ReadEntry>();
    for(FileSpec f : scan.getSelection().getWith(FileSystemInputConfig.class).files){
      s.add(new FSEntry(f, scan.getOutputReference()));
    }
    return s;
  }

  @Override
  public RecordReader getReader(ReadEntry readEntry, ROP parentROP) throws IOException {
    FSEntry e = getReadEntry(FSEntry.class, readEntry);
    
    switch(e.type){
    case JSON:
      return new JSONRecordReader(e.rootPath, dConfig, fs.open(e.path), parentROP);
    default:
      throw new UnsupportedOperationException();
    }
  }
  
  

  
}
