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
package org.apache.drill.exec.store;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

import com.google.common.io.Resources;


public class ClassPathFileSystem extends FileSystem{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ClassPathFileSystem.class);

  static final String ERROR_MSG = "ClassPathFileSystem is read only.";

  private Path working;

  @Override
  public FSDataOutputStream append(Path arg0, int arg1, Progressable arg2) throws IOException {
    throw new IOException(ERROR_MSG);
  }

  @Override
  public FSDataOutputStream create(Path arg0, FsPermission arg1, boolean arg2, int arg3, short arg4, long arg5,
      Progressable arg6) throws IOException {
    throw new IOException(ERROR_MSG);
  }

  @Override
  public boolean delete(Path arg0) throws IOException {
    throw new IOException(ERROR_MSG);
  }

  @Override
  public boolean delete(Path arg0, boolean arg1) throws IOException {
    throw new IOException(ERROR_MSG);
  }

  private String getFileName(Path path){
    String file = path.toString();
    if(file.charAt(0) == '/'){
      file = file.substring(1);
    }
    return file;
  }

  @Override
  public FileStatus getFileStatus(Path arg0) throws IOException {
    String file = getFileName(arg0);

    try{
    URL url = Resources.getResource(file);
    if(url == null){
      throw new IOException(String.format("Unable to find path %s.", arg0.toString()));
    }


    return new FileStatus(Resources.asByteSource(url).size(), false, 1, 8096, System.currentTimeMillis(), arg0);
    }catch(RuntimeException e){
      throw new IOException(String.format("Failure trying to load file %s", arg0), e);
    }
  }

  @Override
  public URI getUri() {
    try {
      return new URI("classpath:///");
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Path getWorkingDirectory() {
    return working;
  }

  @Override
  public FileStatus[] listStatus(Path arg0) throws IOException {
    throw new IOException("ClassPathFileSystem doesn't currently support listing files.");
  }

  @Override
  public boolean mkdirs(Path arg0, FsPermission arg1) throws IOException {
    throw new IOException(ERROR_MSG);
  }

  @Override
  public FSDataInputStream open(Path arg0, int arg1) throws IOException {
    String file = getFileName(arg0);
    URL url = Resources.getResource(file);
    if(url == null){
      throw new IOException(String.format("Unable to find path %s.", arg0.getName()));
    }
    ResourceInputStream ris = new ResourceInputStream(Resources.toByteArray(url));
    return new FSDataInputStream(ris);
  }

  @Override
  public boolean rename(Path arg0, Path arg1) throws IOException {
    throw new IOException(ERROR_MSG);
  }

  @Override
  public void setWorkingDirectory(Path arg0) {
    this.working = arg0;
  }

  public static void main(String[] args) throws Exception{
    URI uri = new URI("classpath:///");
  }
}
