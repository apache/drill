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
package org.apache.drill.exec.store.sys.local;

import static org.apache.drill.exec.ExecConstants.DRILL_SYS_FILE_SUFFIX;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import org.apache.commons.io.IOUtils;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.exec.store.dfs.DrillFileSystem;
import org.apache.drill.exec.store.sys.PStore;
import org.apache.drill.exec.store.sys.PStoreConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

public class FilePStore<V> implements PStore<V> {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FilePStore.class);


  private final Path basePath;
  private final PStoreConfig<V> config;
  private final DrillFileSystem fs;

  public FilePStore(DrillFileSystem fs, Path base, PStoreConfig<V> config) {
    super();
    this.basePath = new Path(base, config.getName());
    this.config = config;
    this.fs = fs;

    try {
      mk(basePath);
    } catch (IOException e) {
      throw new RuntimeException("Failure setting pstore configuration path.");
    }
  }

  private void mk(Path path) throws IOException{
    fs.mkdirs(path);
  }

  public static Path getLogDir(){
    String drillLogDir = System.getenv("DRILL_LOG_DIR");
    if (drillLogDir == null) {
      drillLogDir = "/var/log/drill";
    }
    return new Path(new File(drillLogDir).getAbsoluteFile().toURI());
  }

  public static DrillFileSystem getFileSystem(DrillConfig config, Path root) throws IOException{
    Path blobRoot = root == null ? getLogDir() : root;
    Configuration fsConf = new Configuration();
    if(blobRoot.toUri().getScheme() != null){
      fsConf.set(FileSystem.FS_DEFAULT_NAME_KEY, blobRoot.toUri().toString());
    }


    DrillFileSystem fs = new DrillFileSystem(fsConf);
    fs.mkdirs(blobRoot);
    return fs;
  }

  @Override
  public Iterator<Entry<String, V>> iterator() {
    try{
      List<FileStatus> f = fs.list(false, basePath);
      if (f == null || f.isEmpty()) {
        return Collections.emptyIterator();
      }
      List<String> files = Lists.newArrayList();

      for (FileStatus stat : f) {
        String s = stat.getPath().getName();
        if (s.endsWith(DRILL_SYS_FILE_SUFFIX)) {
          files.add(s.substring(0, s.length() - DRILL_SYS_FILE_SUFFIX.length()));
        }
      }

      Collections.sort(files);
      files = files.subList(0, Math.min(files.size(), config.getMaxIteratorSize()));
      return new Iter(files.iterator());

    }catch(IOException e){
      throw new RuntimeException(e);
    }
  }

  private Path p(String name) throws IOException {
    Preconditions.checkArgument(
        !name.contains("/") &&
        !name.contains(":") &&
        !name.contains(".."));

    Path f = new Path(basePath, name + DRILL_SYS_FILE_SUFFIX);
    // do this to check file name.
    return f;
  }

  public V get(String key) {
    try{
      Path path = p(key);
      if(!fs.exists(path)){
        return null;
      }
    }catch(IOException e){
      throw new RuntimeException(e);
    }

    try (InputStream is = fs.open(p(key))) {
      return config.getSerializer().deserialize(IOUtils.toByteArray(is));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public void put(String key, V value) {
    try (OutputStream os = fs.create(p(key))) {
      IOUtils.write(config.getSerializer().serialize(value), os);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean putIfAbsent(String key, V value) {
    try {
      Path p = p(key);
      if (fs.exists(p)) {
        return false;
      } else {
        put(key, value);
        return true;
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public void delete(String key) {
    try {
      fs.delete(p(key), false);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private class Iter implements Iterator<Entry<String, V>>{

    private Iterator<String> keys;
    private String current;

    public Iter(Iterator<String> keys) {
      super();
      this.keys = keys;
    }

    @Override
    public boolean hasNext() {
      return keys.hasNext();
    }

    @Override
    public Entry<String, V> next() {
      current = keys.next();
      return new DeferredEntry(current);
    }

    @Override
    public void remove() {
      delete(current);
      keys.remove();
    }

    private class DeferredEntry implements Entry<String, V> {

      private String name;


      public DeferredEntry(String name) {
        super();
        this.name = name;
      }

      @Override
      public String getKey() {
        return name;
      }

      @Override
      public V getValue() {
        return get(name);
      }

      @Override
      public V setValue(V value) {
        throw new UnsupportedOperationException();
      }

    }
  }

  @Override
  public void close() {
  }

}
