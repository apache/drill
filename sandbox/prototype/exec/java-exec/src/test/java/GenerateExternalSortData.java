import java.io.BufferedReader;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.nio.file.FileSystems;
import java.nio.file.Files;

import org.apache.hadoop.conf.Configuration;
import org.xerial.snappy.Snappy;

import com.google.common.base.Charsets;
import com.google.protobuf.CodedOutputStream;

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

public class GenerateExternalSortData {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(GenerateExternalSortData.class);
  
  /** Convert sequence file in to compressed columnar format. 
   * 
   * @param args
   * @throws Exception
   */
  public static void main(String[] args) throws Exception{
    int mb = 1024*1024;
    final int blockSize = 1024;
    ByteBuffer keys = ByteBuffer.allocateDirect(2*mb);
    ByteBuffer values = ByteBuffer.allocateDirect(20*mb);
    ByteBuffer fromCompressBuffer = ByteBuffer.allocateDirect(mb);
    ByteBuffer toCompressBuffer = ByteBuffer.allocateDirect(mb);
    
    ByteBuffer valueLengthB = ByteBuffer.allocateDirect(blockSize*4);
    IntBuffer valueLengths = valueLengthB.asIntBuffer();
    //Opaque value stored as len,data.
    
    //
    //Snappy.compress(uncompressed, compressed);
    String file = "/opt/data/tera1gb/part-00000";
    Configuration config = new Configuration();
    //SequenceFile.Reader sf = new SequenceFile.Reader(FileSystem.getLocal(config), new Path(file), config);
    
    BufferedReader reader = Files.newBufferedReader(FileSystems.getDefault().getPath(file), Charsets.UTF_8);
    
    CodedOutputStream cos = CodedOutputStream.newInstance(new BBOutputStream(values));
    
    long originalBytes = 0;
    long compressedBytes = 0;
    String l;
    int round = 0;
    long nanos = 0;
    long x1 = System.nanoTime();
    while((l = reader.readLine()) != null){
      
      byte[] bytes = l.getBytes();
      keys.put(bytes, 0, 10);
      int len = bytes.length - 10;
      originalBytes += len;
      
      
      // Compress the value.
      long n1 = System.nanoTime();
      fromCompressBuffer.put(bytes, 10, len);
      fromCompressBuffer.flip();
      int newLen = Snappy.compress(fromCompressBuffer, toCompressBuffer);
      cos.writeRawVarint32(newLen);
      toCompressBuffer.flip();
      values.put(toCompressBuffer);
      fromCompressBuffer.clear();
      toCompressBuffer.clear();
      nanos += (System.nanoTime() - n1);

      compressedBytes += newLen;
      //valueLengths.put(newLen);
      
      round++;
      
      if(round >= blockSize){
        // flush
        keys.clear();
        values.clear();
        round = 0;
        
      }
      
      
    }
    
    System.out.println("Uncompressed: " + originalBytes);
    System.out.println("Compressed: " + compressedBytes);
    System.out.println("CompressionTime: " + nanos/1000/1000);
    System.out.println("Total Time: " + (System.nanoTime() - x1)/1000/1000);
    
  }
  
  private static void convertToDeltas(IntBuffer b){
    b.flip();
    int min = Integer.MAX_VALUE;
    for(int i =0; i < b.limit(); i++){
      min = Math.min(b.get(i), min);
    }
    
    for(int i =0; i < b.limit(); i++){
      int cur = b.get(i);
      b.put(i, cur - min);
    }
    
    
  }
}
