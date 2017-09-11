
package org.apache.drill.exec.test.generated;

import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.expr.holders.BigIntHolder;
import org.apache.drill.exec.expr.holders.IntHolder;
import org.apache.drill.exec.expr.holders.NullableBigIntHolder;
import org.apache.drill.exec.expr.holders.NullableFloat8Holder;
import org.apache.drill.exec.expr.holders.NullableIntHolder;
import org.apache.drill.exec.physical.impl.aggregate.HashAggTemplate;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.vector.BigIntVector;
import org.apache.drill.exec.vector.IntVector;
import org.apache.drill.exec.vector.NullableBigIntVector;
import org.apache.drill.exec.vector.NullableFloat8Vector;
import org.apache.drill.exec.vector.NullableIntVector;

public class HashAggregatorGen1
    extends HashAggTemplate
{


    public HashAggregatorGen1() {
        try {
            __DRILL_INIT__();
        } catch (SchemaChangeException e) {
            throw new UnsupportedOperationException(e);
        }
    }

    protected HashAggTemplate.BatchHolder newBatchHolder() {
        return new HashAggregatorGen1 .BatchHolder();
    }

    public void doSetup(RecordBatch incoming)
        throws SchemaChangeException
    {
    }

    public int getVectorIndex(int recordIndex)
        throws SchemaChangeException
    {
        {
            return (recordIndex);
        }
    }

    public boolean resetValues()
        throws SchemaChangeException
    {
        {
            return true;
        }
    }

    public void __DRILL_INIT__()
        throws SchemaChangeException
    {
    }

    private final class BatchHolder
        extends HashAggTemplate.BatchHolder
    {

        BigIntHolder work0;
        BigIntVector vv1;
        BigIntHolder work4;
        BigIntVector vv5;
        IntVector vv8;
        NullableBigIntVector vv13;
        IntHolder work16;
        IntVector vv17;
        BigIntHolder work20;
        BigIntVector vv21;
        IntVector vv24;
        NullableIntVector vv29;
        IntHolder work32;
        IntVector vv33;
        BigIntHolder work36;
        BigIntVector vv37;
        IntVector vv40;
        NullableIntVector vv45;
        BigIntHolder work48;
        BigIntVector vv49;
        IntVector vv52;
        BigIntVector vv57;
        BigIntHolder work60;
        BigIntVector vv61;
        BigIntHolder work64;
        BigIntVector vv65;
        BigIntHolder work68;
        BigIntVector vv69;
        IntVector vv72;
        NullableFloat8Vector vv77;

        public BatchHolder() {
            super();
            try {
                __DRILL_INIT__();
            } catch (SchemaChangeException e) {
                throw new UnsupportedOperationException(e);
            }
        }

        public void outputRecordValues(int htRowIdx, int outRowIdx)
            throws SchemaChangeException
        {
            {
                NullableBigIntHolder out12;
                {
                    final NullableBigIntHolder out = new NullableBigIntHolder();
                    work0 .value = vv1 .getAccessor().get((htRowIdx));
                    BigIntHolder value = work0;
                    work4 .value = vv5 .getAccessor().get((htRowIdx));
                    BigIntHolder nonNullCount = work4;
                     
SumFunctions$IntSum_output: {
    if (nonNullCount.value > 0) {
        out.value = value.value;
        out.isSet = 1;
    } else
    {
        out.isSet = 0;
    }
}
 
                    work0 = value;
                    vv1 .getMutator().set((htRowIdx), work0 .value);
                    work4 = nonNullCount;
                    vv5 .getMutator().set((htRowIdx), work4 .value);
                    out12 = out;
                }
                if (!(out12 .isSet == 0)) {
                    vv13 .getMutator().setSafe((outRowIdx), out12 .isSet, out12 .value);
                }
            }
            {
                NullableIntHolder out28;
                {
                    final NullableIntHolder out = new NullableIntHolder();
                    work16 .value = vv17 .getAccessor().get((htRowIdx));
                    IntHolder value = work16;
                    work20 .value = vv21 .getAccessor().get((htRowIdx));
                    BigIntHolder nonNullCount = work20;
                     
MinFunctions$IntMin_output: {
    if (nonNullCount.value > 0) {
        out.value = value.value;
        out.isSet = 1;
    } else
    {
        out.isSet = 0;
    }
}
 
                    work16 = value;
                    vv17 .getMutator().set((htRowIdx), work16 .value);
                    work20 = nonNullCount;
                    vv21 .getMutator().set((htRowIdx), work20 .value);
                    out28 = out;
                }
                if (!(out28 .isSet == 0)) {
                    vv29 .getMutator().setSafe((outRowIdx), out28 .isSet, out28 .value);
                }
            }
            {
                NullableIntHolder out44;
                {
                    final NullableIntHolder out = new NullableIntHolder();
                    work32 .value = vv33 .getAccessor().get((htRowIdx));
                    IntHolder value = work32;
                    work36 .value = vv37 .getAccessor().get((htRowIdx));
                    BigIntHolder nonNullCount = work36;
                     
MaxFunctions$IntMax_output: {
    if (nonNullCount.value > 0) {
        out.value = value.value;
        out.isSet = 1;
    } else
    {
        out.isSet = 0;
    }
}
 
                    work32 = value;
                    vv33 .getMutator().set((htRowIdx), work32 .value);
                    work36 = nonNullCount;
                    vv37 .getMutator().set((htRowIdx), work36 .value);
                    out44 = out;
                }
                if (!(out44 .isSet == 0)) {
                    vv45 .getMutator().setSafe((outRowIdx), out44 .isSet, out44 .value);
                }
            }
            {
                BigIntHolder out56;
                {
                    final BigIntHolder out = new BigIntHolder();
                    work48 .value = vv49 .getAccessor().get((htRowIdx));
                    BigIntHolder value = work48;
                     
CountFunctions$IntCountFunction_output: {
    out.value = value.value;
}
 
                    work48 = value;
                    vv49 .getMutator().set((htRowIdx), work48 .value);
                    out56 = out;
                }
                vv57 .getMutator().setSafe((outRowIdx), out56 .value);
            }
            {
                NullableFloat8Holder out76;
                {
                    final NullableFloat8Holder out = new NullableFloat8Holder();
                    work60 .value = vv61 .getAccessor().get((htRowIdx));
                    BigIntHolder sum = work60;
                    work64 .value = vv65 .getAccessor().get((htRowIdx));
                    BigIntHolder count = work64;
                    work68 .value = vv69 .getAccessor().get((htRowIdx));
                    BigIntHolder nonNullCount = work68;
                     
AvgFunctions$IntAvg_output: {
    if (nonNullCount.value > 0) {
        out.value = sum.value / ((double) count.value);
        out.isSet = 1;
    } else
    {
        out.isSet = 0;
    }
}
 
                    work60 = sum;
                    vv61 .getMutator().set((htRowIdx), work60 .value);
                    work64 = count;
                    vv65 .getMutator().set((htRowIdx), work64 .value);
                    work68 = nonNullCount;
                    vv69 .getMutator().set((htRowIdx), work68 .value);
                    out76 = out;
                }
                if (!(out76 .isSet == 0)) {
                    vv77 .getMutator().setSafe((outRowIdx), out76 .isSet, out76 .value);
                }
            }
        }

        public void setupInterior(RecordBatch incoming, RecordBatch outgoing, VectorContainer aggrValuesContainer)
            throws SchemaChangeException
        {
            {
                int[] fieldIds2 = new int[ 1 ] ;
                fieldIds2 [ 0 ] = 0;
                Object tmp3 = (aggrValuesContainer).getValueAccessorById(BigIntVector.class, fieldIds2).getValueVector();
                if (tmp3 == null) {
                    throw new SchemaChangeException("Failure while loading vector vv1 with id: TypedFieldId [fieldIds=[0], remainder=null].");
                }
                vv1 = ((BigIntVector) tmp3);
                int[] fieldIds6 = new int[ 1 ] ;
                fieldIds6 [ 0 ] = 1;
                Object tmp7 = (aggrValuesContainer).getValueAccessorById(BigIntVector.class, fieldIds6).getValueVector();
                if (tmp7 == null) {
                    throw new SchemaChangeException("Failure while loading vector vv5 with id: TypedFieldId [fieldIds=[1], remainder=null].");
                }
                vv5 = ((BigIntVector) tmp7);
                int vectorSize = 2147483647;
                vectorSize = Math.min(vectorSize, vv1 .getValueCapacity());
                vectorSize = Math.min(vectorSize, vv5 .getValueCapacity());
                work0 = new BigIntHolder();
                work4 = new BigIntHolder();
                for (int drill_internal_i = 0; (drill_internal_i<vectorSize); drill_internal_i += 1) {
                    {
                        /** start SETUP for function sum **/ 
                        {
                            work0 .value = vv1 .getAccessor().get(drill_internal_i);
                            BigIntHolder value = work0;
                            work4 .value = vv5 .getAccessor().get(drill_internal_i);
                            BigIntHolder nonNullCount = work4;
                             
SumFunctions$IntSum_setup: {
    value = new BigIntHolder();
    nonNullCount = new BigIntHolder();
    nonNullCount.value = 0;
    value.value = 0;
}
 
                            work0 = value;
                            vv1 .getMutator().set(drill_internal_i, work0 .value);
                            work4 = nonNullCount;
                            vv5 .getMutator().set(drill_internal_i, work4 .value);
                        }
                        /** end SETUP for function sum **/ 
                    }
                }
                int[] fieldIds9 = new int[ 1 ] ;
                fieldIds9 [ 0 ] = 1;
                Object tmp10 = (incoming).getValueAccessorById(IntVector.class, fieldIds9).getValueVector();
                if (tmp10 == null) {
                    throw new SchemaChangeException("Failure while loading vector vv8 with id: TypedFieldId [fieldIds=[1], remainder=null].");
                }
                vv8 = ((IntVector) tmp10);
                int[] fieldIds14 = new int[ 1 ] ;
                fieldIds14 [ 0 ] = 1;
                Object tmp15 = (outgoing).getValueAccessorById(NullableBigIntVector.class, fieldIds14).getValueVector();
                if (tmp15 == null) {
                    throw new SchemaChangeException("Failure while loading vector vv13 with id: TypedFieldId [fieldIds=[1], remainder=null].");
                }
                vv13 = ((NullableBigIntVector) tmp15);
            }
            {
                int[] fieldIds18 = new int[ 1 ] ;
                fieldIds18 [ 0 ] = 2;
                Object tmp19 = (aggrValuesContainer).getValueAccessorById(IntVector.class, fieldIds18).getValueVector();
                if (tmp19 == null) {
                    throw new SchemaChangeException("Failure while loading vector vv17 with id: TypedFieldId [fieldIds=[2], remainder=null].");
                }
                vv17 = ((IntVector) tmp19);
                int[] fieldIds22 = new int[ 1 ] ;
                fieldIds22 [ 0 ] = 3;
                Object tmp23 = (aggrValuesContainer).getValueAccessorById(BigIntVector.class, fieldIds22).getValueVector();
                if (tmp23 == null) {
                    throw new SchemaChangeException("Failure while loading vector vv21 with id: TypedFieldId [fieldIds=[3], remainder=null].");
                }
                vv21 = ((BigIntVector) tmp23);
                int vectorSize = 2147483647;
                vectorSize = Math.min(vectorSize, vv17 .getValueCapacity());
                vectorSize = Math.min(vectorSize, vv21 .getValueCapacity());
                work16 = new IntHolder();
                work20 = new BigIntHolder();
                for (int drill_internal_i = 0; (drill_internal_i<vectorSize); drill_internal_i += 1) {
                    {
                        /** start SETUP for function min **/ 
                        {
                            work16 .value = vv17 .getAccessor().get(drill_internal_i);
                            IntHolder value = work16;
                            work20 .value = vv21 .getAccessor().get(drill_internal_i);
                            BigIntHolder nonNullCount = work20;
                             
MinFunctions$IntMin_setup: {
    value = new IntHolder();
    nonNullCount = new BigIntHolder();
    nonNullCount.value = 0;
    value.value = Integer.MAX_VALUE;
}
 
                            work16 = value;
                            vv17 .getMutator().set(drill_internal_i, work16 .value);
                            work20 = nonNullCount;
                            vv21 .getMutator().set(drill_internal_i, work20 .value);
                        }
                        /** end SETUP for function min **/ 
                    }
                }
                int[] fieldIds25 = new int[ 1 ] ;
                fieldIds25 [ 0 ] = 1;
                Object tmp26 = (incoming).getValueAccessorById(IntVector.class, fieldIds25).getValueVector();
                if (tmp26 == null) {
                    throw new SchemaChangeException("Failure while loading vector vv24 with id: TypedFieldId [fieldIds=[1], remainder=null].");
                }
                vv24 = ((IntVector) tmp26);
                int[] fieldIds30 = new int[ 1 ] ;
                fieldIds30 [ 0 ] = 2;
                Object tmp31 = (outgoing).getValueAccessorById(NullableIntVector.class, fieldIds30).getValueVector();
                if (tmp31 == null) {
                    throw new SchemaChangeException("Failure while loading vector vv29 with id: TypedFieldId [fieldIds=[2], remainder=null].");
                }
                vv29 = ((NullableIntVector) tmp31);
            }
            {
                int[] fieldIds34 = new int[ 1 ] ;
                fieldIds34 [ 0 ] = 4;
                Object tmp35 = (aggrValuesContainer).getValueAccessorById(IntVector.class, fieldIds34).getValueVector();
                if (tmp35 == null) {
                    throw new SchemaChangeException("Failure while loading vector vv33 with id: TypedFieldId [fieldIds=[4], remainder=null].");
                }
                vv33 = ((IntVector) tmp35);
                int[] fieldIds38 = new int[ 1 ] ;
                fieldIds38 [ 0 ] = 5;
                Object tmp39 = (aggrValuesContainer).getValueAccessorById(BigIntVector.class, fieldIds38).getValueVector();
                if (tmp39 == null) {
                    throw new SchemaChangeException("Failure while loading vector vv37 with id: TypedFieldId [fieldIds=[5], remainder=null].");
                }
                vv37 = ((BigIntVector) tmp39);
                int vectorSize = 2147483647;
                vectorSize = Math.min(vectorSize, vv33 .getValueCapacity());
                vectorSize = Math.min(vectorSize, vv37 .getValueCapacity());
                work32 = new IntHolder();
                work36 = new BigIntHolder();
                for (int drill_internal_i = 0; (drill_internal_i<vectorSize); drill_internal_i += 1) {
                    {
                        /** start SETUP for function max **/ 
                        {
                            work32 .value = vv33 .getAccessor().get(drill_internal_i);
                            IntHolder value = work32;
                            work36 .value = vv37 .getAccessor().get(drill_internal_i);
                            BigIntHolder nonNullCount = work36;
                             
MaxFunctions$IntMax_setup: {
    value = new IntHolder();
    nonNullCount = new BigIntHolder();
    nonNullCount.value = 0;
    value.value = Integer.MIN_VALUE;
}
 
                            work32 = value;
                            vv33 .getMutator().set(drill_internal_i, work32 .value);
                            work36 = nonNullCount;
                            vv37 .getMutator().set(drill_internal_i, work36 .value);
                        }
                        /** end SETUP for function max **/ 
                    }
                }
                int[] fieldIds41 = new int[ 1 ] ;
                fieldIds41 [ 0 ] = 1;
                Object tmp42 = (incoming).getValueAccessorById(IntVector.class, fieldIds41).getValueVector();
                if (tmp42 == null) {
                    throw new SchemaChangeException("Failure while loading vector vv40 with id: TypedFieldId [fieldIds=[1], remainder=null].");
                }
                vv40 = ((IntVector) tmp42);
                int[] fieldIds46 = new int[ 1 ] ;
                fieldIds46 [ 0 ] = 3;
                Object tmp47 = (outgoing).getValueAccessorById(NullableIntVector.class, fieldIds46).getValueVector();
                if (tmp47 == null) {
                    throw new SchemaChangeException("Failure while loading vector vv45 with id: TypedFieldId [fieldIds=[3], remainder=null].");
                }
                vv45 = ((NullableIntVector) tmp47);
            }
            {
                int[] fieldIds50 = new int[ 1 ] ;
                fieldIds50 [ 0 ] = 6;
                Object tmp51 = (aggrValuesContainer).getValueAccessorById(BigIntVector.class, fieldIds50).getValueVector();
                if (tmp51 == null) {
                    throw new SchemaChangeException("Failure while loading vector vv49 with id: TypedFieldId [fieldIds=[6], remainder=null].");
                }
                vv49 = ((BigIntVector) tmp51);
                int vectorSize = 2147483647;
                vectorSize = Math.min(vectorSize, vv49 .getValueCapacity());
                work48 = new BigIntHolder();
                for (int drill_internal_i = 0; (drill_internal_i<vectorSize); drill_internal_i += 1) {
                    {
                        /** start SETUP for function count **/ 
                        {
                            work48 .value = vv49 .getAccessor().get(drill_internal_i);
                            BigIntHolder value = work48;
                             
CountFunctions$IntCountFunction_setup: {
    value = new BigIntHolder();
    value.value = 0;
}
 
                            work48 = value;
                            vv49 .getMutator().set(drill_internal_i, work48 .value);
                        }
                        /** end SETUP for function count **/ 
                    }
                }
                int[] fieldIds53 = new int[ 1 ] ;
                fieldIds53 [ 0 ] = 1;
                Object tmp54 = (incoming).getValueAccessorById(IntVector.class, fieldIds53).getValueVector();
                if (tmp54 == null) {
                    throw new SchemaChangeException("Failure while loading vector vv52 with id: TypedFieldId [fieldIds=[1], remainder=null].");
                }
                vv52 = ((IntVector) tmp54);
                int[] fieldIds58 = new int[ 1 ] ;
                fieldIds58 [ 0 ] = 4;
                Object tmp59 = (outgoing).getValueAccessorById(BigIntVector.class, fieldIds58).getValueVector();
                if (tmp59 == null) {
                    throw new SchemaChangeException("Failure while loading vector vv57 with id: TypedFieldId [fieldIds=[4], remainder=null].");
                }
                vv57 = ((BigIntVector) tmp59);
            }
            {
                int[] fieldIds62 = new int[ 1 ] ;
                fieldIds62 [ 0 ] = 7;
                Object tmp63 = (aggrValuesContainer).getValueAccessorById(BigIntVector.class, fieldIds62).getValueVector();
                if (tmp63 == null) {
                    throw new SchemaChangeException("Failure while loading vector vv61 with id: TypedFieldId [fieldIds=[7], remainder=null].");
                }
                vv61 = ((BigIntVector) tmp63);
                int[] fieldIds66 = new int[ 1 ] ;
                fieldIds66 [ 0 ] = 8;
                Object tmp67 = (aggrValuesContainer).getValueAccessorById(BigIntVector.class, fieldIds66).getValueVector();
                if (tmp67 == null) {
                    throw new SchemaChangeException("Failure while loading vector vv65 with id: TypedFieldId [fieldIds=[8], remainder=null].");
                }
                vv65 = ((BigIntVector) tmp67);
                int[] fieldIds70 = new int[ 1 ] ;
                fieldIds70 [ 0 ] = 9;
                Object tmp71 = (aggrValuesContainer).getValueAccessorById(BigIntVector.class, fieldIds70).getValueVector();
                if (tmp71 == null) {
                    throw new SchemaChangeException("Failure while loading vector vv69 with id: TypedFieldId [fieldIds=[9], remainder=null].");
                }
                vv69 = ((BigIntVector) tmp71);
                int vectorSize = 2147483647;
                vectorSize = Math.min(vectorSize, vv61 .getValueCapacity());
                vectorSize = Math.min(vectorSize, vv65 .getValueCapacity());
                vectorSize = Math.min(vectorSize, vv69 .getValueCapacity());
                work60 = new BigIntHolder();
                work64 = new BigIntHolder();
                work68 = new BigIntHolder();
                for (int drill_internal_i = 0; (drill_internal_i<vectorSize); drill_internal_i += 1) {
                    {
                        /** start SETUP for function avg **/ 
                        {
                            work60 .value = vv61 .getAccessor().get(drill_internal_i);
                            BigIntHolder sum = work60;
                            work64 .value = vv65 .getAccessor().get(drill_internal_i);
                            BigIntHolder count = work64;
                            work68 .value = vv69 .getAccessor().get(drill_internal_i);
                            BigIntHolder nonNullCount = work68;
                             
AvgFunctions$IntAvg_setup: {
    sum = new BigIntHolder();
    count = new BigIntHolder();
    nonNullCount = new BigIntHolder();
    nonNullCount.value = 0;
    sum.value = 0;
    count.value = 0;
}
 
                            work60 = sum;
                            vv61 .getMutator().set(drill_internal_i, work60 .value);
                            work64 = count;
                            vv65 .getMutator().set(drill_internal_i, work64 .value);
                            work68 = nonNullCount;
                            vv69 .getMutator().set(drill_internal_i, work68 .value);
                        }
                        /** end SETUP for function avg **/ 
                    }
                }
                int[] fieldIds73 = new int[ 1 ] ;
                fieldIds73 [ 0 ] = 1;
                Object tmp74 = (incoming).getValueAccessorById(IntVector.class, fieldIds73).getValueVector();
                if (tmp74 == null) {
                    throw new SchemaChangeException("Failure while loading vector vv72 with id: TypedFieldId [fieldIds=[1], remainder=null].");
                }
                vv72 = ((IntVector) tmp74);
                int[] fieldIds78 = new int[ 1 ] ;
                fieldIds78 [ 0 ] = 5;
                Object tmp79 = (outgoing).getValueAccessorById(NullableFloat8Vector.class, fieldIds78).getValueVector();
                if (tmp79 == null) {
                    throw new SchemaChangeException("Failure while loading vector vv77 with id: TypedFieldId [fieldIds=[5], remainder=null].");
                }
                vv77 = ((NullableFloat8Vector) tmp79);
            }
        }

        public void updateAggrValuesInternal(int incomingRowIdx, int htRowIdx)
            throws SchemaChangeException
        {
            {
                IntHolder out11 = new IntHolder();
                {
                    out11 .value = vv8 .getAccessor().get((incomingRowIdx));
                }
                IntHolder in = out11;
                work0 .value = vv1 .getAccessor().get((htRowIdx));
                BigIntHolder value = work0;
                work4 .value = vv5 .getAccessor().get((htRowIdx));
                BigIntHolder nonNullCount = work4;
                 
SumFunctions$IntSum_add: {
    nonNullCount.value = 1;
    value.value += in.value;
}
 
                work0 = value;
                vv1 .getMutator().set((htRowIdx), work0 .value);
                work4 = nonNullCount;
                vv5 .getMutator().set((htRowIdx), work4 .value);
            }
            {
                IntHolder out27 = new IntHolder();
                {
                    out27 .value = vv24 .getAccessor().get((incomingRowIdx));
                }
                IntHolder in = out27;
                work16 .value = vv17 .getAccessor().get((htRowIdx));
                IntHolder value = work16;
                work20 .value = vv21 .getAccessor().get((htRowIdx));
                BigIntHolder nonNullCount = work20;
                 
MinFunctions$IntMin_add: {
    nonNullCount.value = 1;
    value.value = Math.min(value.value, in.value);
}
 
                work16 = value;
                vv17 .getMutator().set((htRowIdx), work16 .value);
                work20 = nonNullCount;
                vv21 .getMutator().set((htRowIdx), work20 .value);
            }
            {
                IntHolder out43 = new IntHolder();
                {
                    out43 .value = vv40 .getAccessor().get((incomingRowIdx));
                }
                IntHolder in = out43;
                work32 .value = vv33 .getAccessor().get((htRowIdx));
                IntHolder value = work32;
                work36 .value = vv37 .getAccessor().get((htRowIdx));
                BigIntHolder nonNullCount = work36;
                 
MaxFunctions$IntMax_add: {
    nonNullCount.value = 1;
    value.value = Math.max(value.value, in.value);
}
 
                work32 = value;
                vv33 .getMutator().set((htRowIdx), work32 .value);
                work36 = nonNullCount;
                vv37 .getMutator().set((htRowIdx), work36 .value);
            }
            {
                IntHolder out55 = new IntHolder();
                {
                    out55 .value = vv52 .getAccessor().get((incomingRowIdx));
                }
                IntHolder in = out55;
                work48 .value = vv49 .getAccessor().get((htRowIdx));
                BigIntHolder value = work48;
                 
CountFunctions$IntCountFunction_add: {
    value.value++;
}
 
                work48 = value;
                vv49 .getMutator().set((htRowIdx), work48 .value);
            }
            {
                IntHolder out75 = new IntHolder();
                {
                    out75 .value = vv72 .getAccessor().get((incomingRowIdx));
                }
                IntHolder in = out75;
                work60 .value = vv61 .getAccessor().get((htRowIdx));
                BigIntHolder sum = work60;
                work64 .value = vv65 .getAccessor().get((htRowIdx));
                BigIntHolder count = work64;
                work68 .value = vv69 .getAccessor().get((htRowIdx));
                BigIntHolder nonNullCount = work68;
                 
AvgFunctions$IntAvg_add: {
    nonNullCount.value = 1;
    sum.value += in.value;
    count.value++;
}
 
                work60 = sum;
                vv61 .getMutator().set((htRowIdx), work60 .value);
                work64 = count;
                vv65 .getMutator().set((htRowIdx), work64 .value);
                work68 = nonNullCount;
                vv69 .getMutator().set((htRowIdx), work68 .value);
            }
        }

        public void __DRILL_INIT__()
            throws SchemaChangeException
        {
        }

    }

}
