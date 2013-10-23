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
import java.nio.channels.FileChannel;
import java.nio.ByteBuffer;
import java.util.List;

import org.apache.drill.exec.ExecConstants;
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

public class TraceRecordBatch extends AbstractSingleRecordBatch<Trace>
{
    static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TraceRecordBatch.class);

    private SelectionVector2 sv = null;

    /* Tag associated with each trace operator */
    final String traceTag;

    /* Location where the log should be dumped */
    private final String logLocation;

    public TraceRecordBatch(Trace pop, RecordBatch incoming, FragmentContext context)
    {
        super(pop, context, incoming);
        this.traceTag = pop.traceTag;
        logLocation = context.getConfig().getString(ExecConstants.TRACE_DUMP_DIRECTORY);
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
     */
    @Override
    protected void doWork()
    {
        /* get the selection vector mode */
        SelectionVectorMode svMode = incoming.getSchema().getSelectionVectorMode();

        /* Get the array of buffers from the incoming record batch */
        WritableBatch batch = incoming.getWritableBatch();

        BufferAllocator allocator = context.getAllocator();
        ByteBuf[] incomingBuffers = batch.getBuffers();
        RecordBatchDef batchDef = batch.getDef();

        /* Total length of buffers across all value vectors */
        int totalBufferLength = 0;

        String fileName = getFileName();

        try
        {
            File file = new File(fileName);

            if (!file.exists())
                file.createNewFile();

            FileOutputStream fos = new FileOutputStream(file, true);

            /* Write the metadata to the file */
            batchDef.writeDelimitedTo(fos);

            FileChannel fc = fos.getChannel();

            /* If we have a selection vector, dump it to file first */
            if (svMode == SelectionVectorMode.TWO_BYTE)
            {
                SelectionVector2 incomingSV2 = incoming.getSelectionVector2();
                int recordCount = incomingSV2.getCount();
                int sv2Size = recordCount * SelectionVector2.RECORD_SIZE;

                ByteBuf buf = incomingSV2.getBuffer();

                /* For writing to the selection vectors we use
                 * setChar() method which does not modify the
                 * reader and writer index. To copy the entire buffer
                 * without having to get each byte individually we need
                 * to set the writer index
                 */
                buf.writerIndex(sv2Size);

                /* dump the selection vector to log */
                dumpByteBuf(fc, buf);

                if (sv == null)
                    sv = new SelectionVector2(allocator);

                sv.setRecordCount(recordCount);

                /* create our selection vector from the
                 * incoming selection vector's buffer
                 */
                sv.setBuffer(buf);

                buf.release();
            }

            /* For each buffer dump it to log and compute total length */
            for (ByteBuf buf : incomingBuffers)
            {
                /* dump the buffer into the file channel */
                dumpByteBuf(fc, buf);

                /* Reset reader index on the ByteBuf so we can read it again */
                buf.resetReaderIndex();

                /* compute total length of buffer, will be used when
                 * we create a compound buffer
                 */
                totalBufferLength += buf.readableBytes();
            }

            fc.close();
            fos.close();

        } catch (IOException e)
        {
            logger.error("Unable to write buffer to file: " + fileName);
        }

        /* allocate memory for the compound buffer, compound buffer
         * will contain the data from all the buffers across all the
         * value vectors
         */
        ByteBuf byteBuf = allocator.buffer(totalBufferLength);

        /* Copy data from each buffer into the compound buffer */
        for (int i = 0; i < incomingBuffers.length; i++)
        {
            byteBuf.writeBytes(incomingBuffers[i], incomingBuffers[i].readableBytes());
        }

        List<FieldMetadata> fields = batchDef.getFieldList();

        int bufferOffset = 0;

        /* For each value vector slice up the appropriate size from
         * the compound buffer and load it into the value vector
         */
        int vectorIndex = 0;
        for(VectorWrapper<?> vv : container)
        {
            FieldMetadata fmd = fields.get(vectorIndex);
            ValueVector v = vv.getValueVector();
            v.load(fmd, byteBuf.slice(bufferOffset, fmd.getBufferLength()));
            vectorIndex++;
            bufferOffset += fmd.getBufferLength();
        }

        container.buildSchema(svMode);

        /* Set the record count in the value vector */
        for(VectorWrapper<?> v : container)
        {
            ValueVector.Mutator m = v.getValueVector().getMutator();
            m.setValueCount(incoming.getRecordCount());
        }
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

        return new String(logLocation + "/" + traceTag + "_" + qid + "_" + majorFragmentId + "_" + minorFragmentId);
    }

    private void dumpByteBuf(FileChannel fc, ByteBuf buf) throws IOException
    {
        int bufferLength = buf.readableBytes();

        byte[] byteArray = new byte[bufferLength];

        /* Transfer bytes to a byte array */
        buf.readBytes(byteArray);

        /* Drop the byte array into the file channel */
        fc.write(ByteBuffer.wrap(byteArray));
    }
}
