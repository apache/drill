package org.apache.hadoop.hive.ql.io.orc;

import com.beust.jcommander.internal.Maps;
import com.google.protobuf.CodedInputStream;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

public class OrcInternals {
    public static class RecordReaderImplBridge {
        private final FSDataInputStream file;
        RecordReaderImpl impl;
        private org.apache.hadoop.hive.ql.io.orc.CompressionCodec codec;
        private int bufferSize;
        private OrcProto.Footer footer;
        private Map<StreamName, InStream> streams;

        public RecordReaderImplBridge(FileSystem fs, Path path) throws IOException {
            this.file = fs.open(path);
            this.footer = readFooter(fs, path);
            //this.impl = new RecordReaderImpl(footer.getStripesList(), fs, path, )
            this.streams = Maps.newHashMap();
        }

        public void close() throws IOException {
            file.close();
        }

        private OrcProto.Footer readFooter(FileSystem fs, Path path) throws IOException {
            try (FSDataInputStream file = fs.open(path)) {
                long size = fs.getFileStatus(path).getLen();
                int readSize = (int) Math.min(size, 16 * 1024);
                file.seek(size - readSize);
                ByteBuffer buffer = ByteBuffer.allocate(readSize);
                file.readFully(buffer.array(), buffer.arrayOffset() + buffer.position(),
                        buffer.remaining());
                int psLen = buffer.get(readSize - 1);
                int psOffset = readSize - 1 - psLen;
                CodedInputStream in = CodedInputStream.newInstance(buffer.array(),
                        buffer.arrayOffset() + psOffset, psLen);
                OrcProto.PostScript ps = OrcProto.PostScript.parseFrom(in);
                int footerSize = (int) ps.getFooterLength();
                bufferSize = (int) ps.getCompressionBlockSize();
                CompressionKind compressionKind;
                switch (ps.getCompression()) {
                    case NONE:
                        compressionKind = CompressionKind.NONE;
                        break;
                    case ZLIB:
                        compressionKind = CompressionKind.ZLIB;
                        break;
                    case SNAPPY:
                        compressionKind = CompressionKind.SNAPPY;
                        break;
                    case LZO:
                        compressionKind = CompressionKind.LZO;
                        break;
                    default:
                        throw new IllegalArgumentException("Unknown compression");
                }
                codec = WriterImpl.createCodec(compressionKind);
                int extra = Math.max(0, psLen + 1 + footerSize - readSize);
                if (extra > 0) {
                    file.seek(size - readSize - extra);
                    ByteBuffer extraBuf = ByteBuffer.allocate(extra + readSize);
                    file.readFully(extraBuf.array(),
                            extraBuf.arrayOffset() + extraBuf.position(), extra);
                    extraBuf.position(extra);
                    extraBuf.put(buffer);
                    buffer = extraBuf;
                    buffer.position(0);
                    buffer.limit(footerSize);
                } else {
                    buffer.position(psOffset - footerSize);
                    buffer.limit(psOffset);
                }
                InputStream inStream = InStream.create("footer", new ByteBuffer[]{buffer},
                        new long[]{0L}, footerSize, codec, bufferSize);
                return OrcProto.Footer.parseFrom(inStream);
            }
        }


        public void readStripe(StripeInformation stripeInfo, ColumnReader columnReader) throws IOException {
            OrcProto.StripeFooter footer = readStripeFooter(stripeInfo);
            readStreamsFromStripe(stripeInfo, footer);
            handleStripeData(stripeInfo, footer, columnReader);
            impl.nextBatch(null);
        }

        public OrcProto.StripeFooter readStripeFooter(StripeInformation stripeInfo) throws IOException {
            return impl.readStripeFooter(stripeInfo);
        }

        private void readStreamsFromStripe(StripeInformation stripe,
                                          OrcProto.StripeFooter stripeFooter) throws IOException {
            this.streams.clear();
            byte[] buffer =
                    new byte[(int) (stripe.getDataLength())];
            file.seek(stripe.getOffset() + stripe.getIndexLength());
            file.readFully(buffer, 0, buffer.length);
            int sectionOffset = 0;
            for(OrcProto.Stream section : stripeFooter.getStreamsList()) {
                if (StreamName.getArea(section.getKind()) == StreamName.Area.DATA) {
                    int sectionLength = (int) section.getLength();
                    ByteBuffer sectionBuffer = ByteBuffer.wrap(buffer, sectionOffset,
                            sectionLength);
                    StreamName name = new StreamName(section.getColumn(),
                            section.getKind());
                    streams.put(name,
                            InStream.create(name.toString(), new ByteBuffer[]{sectionBuffer},
                                    new long[]{0}, sectionLength, codec, bufferSize));
                    sectionOffset += sectionLength;
                }
            }
        }

        public List<OrcProto.Type> getColumns() {
            return footer.getTypesList();
        }

        private void handleStripeData(StripeInformation stripe,
                                     OrcProto.StripeFooter stripeFooter,
                                     ColumnReader columnReader) throws IOException {
            List<OrcProto.ColumnEncoding> encodings = stripeFooter.getColumnsList();
            columnReader.readStripe(streams, encodings);
        }
    }
}
