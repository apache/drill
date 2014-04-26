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

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * This class provides a Syncable local extension of the hadoop FileSystem
 */
public class LocalSyncableFileSystem extends FileSystem {

  @Override
  public URI getUri() {
    try {
      return new URI("drill-local:///");
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public FSDataInputStream open(Path path, int i) throws IOException {
    return new FSDataInputStream(new LocalInputStream(path));
  }

  @Override
  public FSDataOutputStream create(Path path, FsPermission fsPermission, boolean b, int i, short i2, long l, Progressable progressable) throws IOException {
    return new FSDataOutputStream(new LocalSyncableOutputStream(path));
  }

  @Override
  public FSDataOutputStream append(Path path, int i, Progressable progressable) throws IOException {
    throw new IOException("Append is not supported in LocalSyncableFilesystem");
  }

  @Override
  public boolean rename(Path path, Path path2) throws IOException {
    throw new IOException("Rename not supported");
  }

  @Override
  public boolean delete(Path path) throws IOException {
    File file = new File(path.toString());
    return file.delete();
  }

  @Override
  public boolean delete(Path path, boolean b) throws IOException {
    File file = new File(path.toString());
    if (b) {
      if (file.isDirectory()) {
        FileUtils.deleteDirectory(file);
      } else {
        file.delete();
      }
    } else if (file.isDirectory()) {
      throw new IOException("Cannot delete directory");
    }
    file.delete();
    return true;
  }

  @Override
  public FileStatus[] listStatus(Path path) throws IOException {
    throw new IOException("listStatus not supported");
  }

  @Override
  public void setWorkingDirectory(Path path) {
  }

  @Override
  public Path getWorkingDirectory() {
    return null;
  }

  @Override
  public boolean mkdirs(Path path, FsPermission fsPermission) throws IOException {
    return new File(path.toString()).mkdirs();
  }

  @Override
  public FileStatus getFileStatus(Path path) throws IOException {
    return null;
  }

  public class LocalSyncableOutputStream extends OutputStream implements Syncable {
    private FileOutputStream fos;
    private BufferedOutputStream output;

    public LocalSyncableOutputStream(Path path) throws FileNotFoundException {
      File dir = new File(path.getParent().toString());
      if (!dir.exists()) {
        boolean success = dir.mkdirs();
        if (!success) {
          throw new FileNotFoundException("failed to create parent directory");
        }
      }
      fos = new FileOutputStream(new File(path.toString()));
      output = new BufferedOutputStream(fos, 64*1024);
    }

    @Override
    public void sync() throws IOException {
      output.flush();
      fos.getFD().sync();
    }

    @Override
    public void write(int b) throws IOException {
      output.write(b);
    }
  }

  public class LocalInputStream extends InputStream implements Seekable, PositionedReadable {

    private BufferedInputStream input;

    public LocalInputStream(Path path)  throws IOException {
      input = new BufferedInputStream(new FileInputStream(path.toString()), 1024*1024);
    }

    @Override
    public int read(long l, byte[] bytes, int i, int i2) throws IOException {
      throw new IOException("unsupported operation");
    }

    @Override
    public void readFully(long l, byte[] bytes, int i, int i2) throws IOException {
      throw new IOException("unsupported operation");
    }

    @Override
    public void readFully(long l, byte[] bytes) throws IOException {
      throw new IOException("unsupported operation");
    }

    @Override
    public void seek(long l) throws IOException {
      input.reset();
      input.skip(l);
    }

    @Override
    public long getPos() throws IOException {
      throw new IOException("getPos not supported");
    }

    @Override
    public boolean seekToNewSource(long l) throws IOException {
      throw new IOException("seekToNewSource not supported");
    }

    @Override
    public int read() throws IOException {
      byte[] b = new byte[1];
      input.read(b);
      return (int) b[0] & 0xFF;
    }
  }
}
