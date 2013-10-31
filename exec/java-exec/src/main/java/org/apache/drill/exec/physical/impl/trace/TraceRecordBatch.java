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

package org.apache.drill.exec.physical.impl.trace;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.FileChannel;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Formatter;

import com.google.common.collect.Iterators;
import io.netty.buffer.CompositeByteBuf;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.cache.VectorAccessibleSerializable;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.config.Trace;
import org.apache.drill.exec.record.*;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;
import org.apache.drill.exec.record.selection.SelectionVector2;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.proto.ExecProtos.FragmentHandle;
import org.apache.drill.exec.proto.helper.QueryIdHelper;
import org.apache.drill.exec.proto.UserBitShared.RecordBatchDef;
import org.apache.drill.exec.proto.UserBitShared.FieldMetadata;
import org.apache.drill.exec.proto.SchemaDefProtos.FieldDef;

import io.netty.buffer.ByteBuf;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/* TraceRecordBatch contains value vectors which are exactly the same
 * as the incoming record batch's value vectors. If the incoming
 * record batch has a selection vector (type 2) then TraceRecordBatch
 * will also contain a selection vector.
 *
 * Purpose of this record batch is to dump the data associated with all
 * the value vectors and selection vector to disk.
 *
 * This record batch does not modify any data or schema, it simply
 * consumes the incoming record batch's data, dump to disk and pass the
 * same set of value vectors (and selection vectors) to its parent record
 * batch
 */
public class TraceRecordBatch extends AbstractSingleRecordBatch<Trace>
{
    static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TraceRecordBatch.class);

    private SelectionVector2 sv = null;

    /* Tag associated with each trace operator */
    final String traceTag;

    /* Location where the log should be dumped */
    private final String logLocation;

    /* File descriptors needed to be able to dump to log file */
    private OutputStream fos;

    public TraceRecordBatch(Trace pop, RecordBatch incoming, FragmentContext context)
    {
        super(pop, context, incoming);
        this.traceTag = pop.traceTag;
        logLocation = context.getConfig().getString(ExecConstants.TRACE_DUMP_DIRECTORY);

        String fileName = getFileName();

        /* Create the log file we will dump to and initialize the file descriptors */
        try
        {
          Configuration conf = new Configuration();
          conf.set("fs.name.default", ExecConstants.TRACE_DUMP_FILESYSTEM);
          FileSystem fs = FileSystem.get(conf);

            /* create the file */
          fos = fs.create(new Path(fileName));
        } catch (IOException e)
        {
            logger.error("Unable to create file: " + fileName);
        }
    }

    @Override
    public int getRecordCount()
    {
        if (sv == null)
            return incoming.getRecordCount();
        else
           return sv.getCount();
    }

    /**
     * Function is invoked for every record batch and it simply
     * dumps the buffers associated with all the value vectors in
     * this record batch to a log file.
     *
     * Function is divided into three main parts
     *   1. Get all the buffers(ByteBuf's) associated with incoming
     *      record batch's value vectors and selection vector
     *   2. Dump these buffers to the log file (performed by writeToFile())
     *   3. Construct the record batch with these buffers to look exactly like
     *      the incoming record batch (performed by reconstructRecordBatch())
     */
    @Override
    protected void doWork()
    {
      VectorAccessibleSerializable wrap = new VectorAccessibleSerializable(incoming,
            incoming.getSchema().getSelectionVectorMode() == SelectionVectorMode.TWO_BYTE ? incoming.getSelectionVector2() : null,
            context.getAllocator());
      wrap.retain(container);

      try {
        wrap.writeToStream(fos);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      sv = wrap.getSv2();
    }

    @Override
    protected void setupNewSchema() throws SchemaChangeException
    {
        /* Trace operator does not deal with hyper vectors yet */
        if (incoming.getSchema().getSelectionVectorMode() == SelectionVectorMode.FOUR_BYTE)
            throw new SchemaChangeException("Trace operator does not work with hyper vectors");

        /* we have a new schema, clear our existing container to
         * load the new value vectors
         */
        container.clear();

        /* Add all the value vectors in the container */
        for(VectorWrapper<?> vv : incoming)
        {
            TransferPair tp = vv.getValueVector().getTransferPair();
            container.add(tp.getTo());
        }
    }

    @Override
    public SelectionVector2 getSelectionVector2() {
        return sv;
    }

    private String getFileName()
    {
        /* From the context, get the query id, major fragment id,
         * minor fragment id. This will be used as the file name
         * to which we will dump the incoming buffer data
         */
        FragmentHandle handle = incoming.getContext().getHandle();

        String qid = QueryIdHelper.getQueryId(handle.getQueryId());

        int majorFragmentId = handle.getMajorFragmentId();
        int minorFragmentId = handle.getMinorFragmentId();

        String fileName = String.format("%s//%s_%s_%s_%s", logLocation, qid, majorFragmentId, minorFragmentId, traceTag);

        return fileName;
    }


    @Override
    protected void cleanup()
    {
        /* Release the selection vector */
        if (sv != null)
          sv.clear();

        /* Close the file descriptors */
        try
        {
            fos.close();
        } catch (IOException e)
        {
            logger.error("Unable to close file descriptors for file: " + getFileName());
        }
    }

}
