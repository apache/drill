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
package org.apache.drill.exec.work.batch;

import io.netty.buffer.ByteBuf;

import java.io.IOException;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.proto.BitData;
import org.apache.drill.exec.proto.ExecProtos;
import org.apache.drill.exec.proto.helper.QueryIdHelper;
import org.apache.drill.exec.record.RawFragmentBatch;
import org.apache.drill.exec.rpc.RemoteConnection;
import org.apache.drill.exec.store.LocalSyncableFileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Queues;

/**
 * This implementation of RawBatchBuffer starts writing incoming batches to disk once the buffer size reaches a threshold.
 * The order of the incoming buffers is maintained.
 */
public class SpoolingRawBatchBuffer implements RawBatchBuffer {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SpoolingRawBatchBuffer.class);

  private static String HADOOP_FILESYSTEM_DEFAULT_NAME = "fs.default.name";
  private static String DRILL_LOCAL_IMPL_STRING = "fs.drill-local.impl";
  private static final float STOP_SPOOLING_FRACTION = (float) 0.5;

  private final LinkedBlockingDeque<RawFragmentBatchWrapper> buffer = Queues.newLinkedBlockingDeque();
  private volatile boolean finished = false;
  private volatile long queueSize = 0;
  private long threshold;
  private FragmentContext context;
  private volatile AtomicBoolean spooling = new AtomicBoolean(false);
  private FileSystem fs;
  private Path path;
  private FSDataOutputStream outputStream;
  private FSDataInputStream inputStream;

  public SpoolingRawBatchBuffer(FragmentContext context) throws IOException {
    this.context = context;
    this.threshold = context.getConfig().getLong(ExecConstants.SPOOLING_BUFFER_MEMORY);
    Configuration conf = new Configuration();
    conf.set(HADOOP_FILESYSTEM_DEFAULT_NAME, context.getConfig().getString(ExecConstants.TEMP_FILESYSTEM));
    conf.set(DRILL_LOCAL_IMPL_STRING, LocalSyncableFileSystem.class.getName());
    this.fs = FileSystem.get(conf);
    this.path = new Path(getDir(), getFileName());
  }

  public static List<String> DIRS = DrillConfig.create().getStringList(ExecConstants.TEMP_DIRECTORIES);

  public static String getDir() {
    Random random = new Random();
    return DIRS.get(random.nextInt(DIRS.size()));
  }

  @Override
  public synchronized void enqueue(RawFragmentBatch batch) throws IOException {
    RawFragmentBatchWrapper wrapper;
    boolean spool = spooling.get();
    wrapper = new RawFragmentBatchWrapper(batch, !spool);
    queueSize += wrapper.getBodySize();
    if (spool) {
      if (outputStream == null) {
        outputStream = fs.create(path);
      }
      wrapper.writeToStream(outputStream);
    }
    buffer.add(wrapper);
    if (!spool && queueSize > threshold) {
      logger.debug("Buffer size {} greater than threshold {}. Start spooling to disk", queueSize, threshold);
      spooling.set(true);
    }
  }

  @Override
  public void kill(FragmentContext context) {
    cleanup();
  }

  
  @Override
  public void finished() {
    finished = true;
  }

  @Override
  public RawFragmentBatch getNext() throws IOException {
    boolean spool = spooling.get();
    RawFragmentBatchWrapper w = buffer.poll();
    RawFragmentBatch batch;
    if(w == null && !finished){
      try {
        w = buffer.take();
        batch = w.get();
        queueSize -= w.getBodySize();
        return batch;
      } catch (InterruptedException e) {
        cleanup();
        return null;
      }
    }
    if (w == null) {
      cleanup();
      return null;
    }

    batch = w.get();
    queueSize -= w.getBodySize();
    assert queueSize >= 0;
    if (spool && queueSize < threshold * STOP_SPOOLING_FRACTION) {
      logger.debug("buffer size {} less than {}x threshold. Stop spooling.", queueSize, STOP_SPOOLING_FRACTION);
      spooling.set(false);
    }
    return batch;
  }

  private void cleanup() {
    try {
      if (outputStream != null) {
        outputStream.close();
      }
      if (inputStream != null) {
        inputStream.close();
      }
    } catch (IOException e) {
      logger.warn("Failed to cleanup I/O streams", e);
    }
    if (context.getConfig().getBoolean(ExecConstants.SPOOLING_BUFFER_DELETE)) {
      try {
        fs.delete(path,false);
      } catch (IOException e) {
        logger.warn("Failed to delete temporary files", e);
      }
      logger.debug("Deleted file {}", path.toString());
    }
  }

  private class RawFragmentBatchWrapper {
    private RawFragmentBatch batch;
    private boolean available;
    private CountDownLatch latch = new CountDownLatch(1);
    private int bodyLength;

    public RawFragmentBatchWrapper(RawFragmentBatch batch, boolean available) {
      Preconditions.checkNotNull(batch);
      this.batch = batch;
      this.available = available;
    }

    public boolean isNull() {
      return batch == null;
    }

    public RawFragmentBatch get() throws IOException {
      if (available) {
        return batch;
      } else {
        if (inputStream == null) {
          inputStream = fs.open(path);
        }
        readFromStream(inputStream);
        available = true;
        return batch;
      }
    }

    public long getBodySize() {
      if (batch.getBody() == null) {
        return 0;
      }
      assert batch.getBody().readableBytes() >= 0;
      return batch.getBody().readableBytes();
    }

    public void writeToStream(FSDataOutputStream stream) throws IOException {
      Stopwatch watch = new Stopwatch();
      watch.start();
      available = false;
      batch.getHeader().writeDelimitedTo(stream);
      ByteBuf buf = batch.getBody();
      if (buf == null) {
        bodyLength = 0;
        return;
      }
      bodyLength = buf.readableBytes();
      buf.getBytes(0, stream, bodyLength);
      stream.sync();
      long t = watch.elapsed(TimeUnit.MICROSECONDS);
      logger.debug("Took {} us to spool {} to disk. Rate {} mb/s", t, bodyLength, bodyLength / t);
      buf.release();
    }

    public void readFromStream(FSDataInputStream stream) throws IOException {
      Stopwatch watch = new Stopwatch();
      watch.start();
      BitData.FragmentRecordBatch header = BitData.FragmentRecordBatch.parseDelimitedFrom(stream);
      ByteBuf buf = context.getAllocator().buffer(bodyLength);
      buf.writeBytes(stream, bodyLength);
      batch = new RawFragmentBatch(null, header, buf);
      available = true;
      latch.countDown();
      long t = watch.elapsed(TimeUnit.MICROSECONDS);
      logger.debug("Took {} us to read {} from disk. Rate {} mb/s", t, bodyLength, bodyLength / t);
    }
  }

  private String getFileName() {
    ExecProtos.FragmentHandle handle = context.getHandle();

    String qid = QueryIdHelper.getQueryId(handle.getQueryId());

    int majorFragmentId = handle.getMajorFragmentId();
    int minorFragmentId = handle.getMinorFragmentId();

    String fileName = String.format("%s_%s_%s", qid, majorFragmentId, minorFragmentId);

    return fileName;
  }
}
