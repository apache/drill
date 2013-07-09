package org.apache.drill.exec.test.generated;

import org.apache.drill.exec.compile.JaninoClassCompiler;


public class TestGeneratedClass {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestGeneratedClass.class);
  public static void main(String[] args) throws Exception{
    String s = "package org.apache.drill.exec.test.generated;\n\nimport org.apache.drill.exec.expr.holders.LongHolder;\nimport org.apache.drill.exec.ops.FragmentContext;\nimport org.apache.drill.exec.record.RecordBatch;\nimport org.apache.drill.exec.record.vector.NullableFixed8;\n\npublic class Test1 {\n\n    NullableFixed8 vv0;\n    NullableFixed8 vv5;\n    NullableFixed8 vv7;\n    NullableFixed8 vv12;\n\n    public void setupEvaluators(FragmentContext context, RecordBatch incoming, RecordBatch outgoing) {\n        {\n            {\n                Object obj1 = (outgoing).getValueVectorById(1, NullableFixed8 .class);\n                vv0 = ((NullableFixed8) obj1);\n                Object obj6 = (outgoing).getValueVectorById(0, NullableFixed8 .class);\n                vv5 = ((NullableFixed8) obj6);\n            }\n            {\n                Object obj8 = (outgoing).getValueVectorById(1, NullableFixed8 .class);\n                vv7 = ((NullableFixed8) obj8);\n                Object obj13 = (outgoing).getValueVectorById(1, NullableFixed8 .class);\n                vv12 = ((NullableFixed8) obj13);\n            }\n        }\n    }\n\n    public void doPerRecordWork(int inIndex, int outIndex) {\n        {\n            {\n                LongHolder out2 = new LongHolder();\n                out2 .value = vv0 .get((inIndex));\n                LongHolder out3 = new LongHolder();\n                out3 .value = 1L;\n                LongHolder out4 = new LongHolder();\n                {\n                    final LongHolder out = new LongHolder();\n                    final LongHolder left = out2;\n                    final LongHolder right = out3;\n                        out.value = left.value + right.value;\n\n                    out4 = out;\n                }\n                vv5 .set((outIndex), out4 .value);\n            }\n            {\n                LongHolder out9 = new LongHolder();\n                out9 .value = vv7 .get((inIndex));\n                LongHolder out10 = new LongHolder();\n                out10 .value = 2L;\n                LongHolder out11 = new LongHolder();\n                {\n                    final LongHolder out = new LongHolder();\n                    final LongHolder left = out9;\n                    final LongHolder right = out10;\n                        out.value = left.value + right.value;\n\n                    out11 = out;\n                }\n                vv12 .set((outIndex), out11 .value);\n            }\n        }\n    }\n\n}";
    JaninoClassCompiler cc = new JaninoClassCompiler(ClassLoader.getSystemClassLoader());
    byte[] b = cc.getClassByteCode("Test12", s);
  }
  
  
}
