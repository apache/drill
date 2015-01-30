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
package org.apache.drill.exec.compile.bytecode;

import java.io.File;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.URL;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.exec.compile.QueryClassLoader;
import org.apache.drill.exec.server.options.SystemOptionManager;
import org.apache.drill.exec.store.sys.local.LocalPStoreProvider;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.util.ASMifier;
import org.objectweb.asm.util.CheckClassAdapter;
import org.objectweb.asm.util.Textifier;
import org.objectweb.asm.util.TraceClassVisitor;

import com.google.common.io.Files;
import com.google.common.io.Resources;

public class ReplaceMethodInvoke {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ReplaceMethodInvoke.class);

  @SuppressWarnings("unchecked")
  public static void main(String[] args) throws Exception{
    String e = "org/apache/drill/ExampleReplaceable.class";
    String h = "org/apache/drill/exec/physical/impl/aggregate/HashAggTemplate.class";
    String k = "org/apache/drill/Pickle$OutgoingBatch.class";
    String k2 = "org/apache/drill/Pickle.class";
    String r = "org/apache/drill/exec/test/generated/FiltererGen0.class";
    String p = "org/apache/drill/exec/test/generated/PartitionerGen4.class";
    URL url = Resources.getResource(k2);
    byte[] clazz = Resources.toByteArray(url);
    ClassReader cr = new ClassReader(clazz);

    ClassWriter cw = writer();
    TraceClassVisitor visitor = new TraceClassVisitor(cw, new Textifier(), new PrintWriter(System.out));
    //getTracer(false)
    ValueHolderReplacementVisitor v2 = new ValueHolderReplacementVisitor(visitor);
    cr.accept(v2, ClassReader.EXPAND_FRAMES );//| ClassReader.SKIP_DEBUG);

    byte[] output = cw.toByteArray();
    Files.write(output, new File("/src/scratch/bytes/S.class"));
    check(output);


    DrillConfig c = DrillConfig.forClient();
    SystemOptionManager m = new SystemOptionManager(c, new LocalPStoreProvider(c));
    m.init();
    QueryClassLoader ql = new QueryClassLoader(DrillConfig.create(), m);
    ql.injectByteCode("org.apache.drill.Pickle$OutgoingBatch", output);
    Class<?> clz = ql.loadClass("org.apache.drill.Pickle$OutgoingBatch");
    clz.getMethod("x").invoke(null);

  }


  private static final void check(byte[] b) {
    ClassReader cr = new ClassReader(b);
    ClassWriter cw = writer();
    ClassVisitor cv = new CheckClassAdapter(cw);
    cr.accept(cv, 0);

    StringWriter sw = new StringWriter();
    PrintWriter pw = new PrintWriter(sw);
    CheckClassAdapter.verify(new ClassReader(cw.toByteArray()), false, pw);

    assert sw.toString().length() == 0 : sw.toString();
  }

  private static ClassWriter writer() {
    ClassWriter cw = new ClassWriter(ClassWriter.COMPUTE_FRAMES);
    return cw;
  }

  private static ClassVisitor getTracer(boolean asm) {
    if (asm) {
      return new TraceClassVisitor(null, new ASMifier(), new PrintWriter(System.out));
    } else {
      return new TraceClassVisitor(null, new Textifier(), new PrintWriter(System.out));
    }
  }



}
