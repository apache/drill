
package org.apache.drill.exec.test.generated;

import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.expr.holders.IntHolder;
import org.apache.drill.exec.physical.impl.common.HashTableTemplate;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.vector.IntVector;

public class HashTableGen2
    extends HashTableTemplate
{

    IntVector vv0;
    IntHolder constant5;

    public HashTableGen2() {
        try {
            __DRILL_INIT__();
        } catch (SchemaChangeException e) {
            throw new UnsupportedOperationException(e);
        }
    }

    protected HashTableTemplate.BatchHolder newBatchHolder(int arg1) {
        return new HashTableGen2 .BatchHolder(arg1);
    }

    public void doSetup(RecordBatch incomingBuild, RecordBatch incomingProbe)
        throws SchemaChangeException
    {
        {
            int[] fieldIds1 = new int[ 1 ] ;
            fieldIds1 [ 0 ] = 0;
            Object tmp2 = (incomingBuild).getValueAccessorById(IntVector.class, fieldIds1).getValueVector();
            if (tmp2 == null) {
                throw new SchemaChangeException("Failure while loading vector vv0 with id: TypedFieldId [fieldIds=[0], remainder=null].");
            }
            vv0 = ((IntVector) tmp2);
            IntHolder out4 = new IntHolder();
            out4 .value = 0;
            constant5 = out4;
            /** start SETUP for function hash32 **/ 
            {
                IntHolder seed = constant5;
                 {}
            }
            /** end SETUP for function hash32 **/ 
        }
    }

    public int getHashBuild(int incomingRowIdx)
        throws SchemaChangeException
    {
        {
            IntHolder out3 = new IntHolder();
            {
                out3 .value = vv0 .getAccessor().get((incomingRowIdx));
            }
            //---- start of eval portion of hash32 function. ----//
            IntHolder out6 = new IntHolder();
            {
                final IntHolder out = new IntHolder();
                IntHolder in = out3;
                IntHolder seed = constant5;
                 
Hash32FunctionsWithSeed$IntHash_eval: {
    out.value = org.apache.drill.exec.expr.fn.impl.HashHelper.hash32(in.value, seed.value);
}
 
                out6 = out;
            }
            //---- end of eval portion of hash32 function. ----//
            return out6 .value;
        }
    }

    public int getHashProbe(int incomingRowIdx)
        throws SchemaChangeException
    {
        {
            return  0;
        }
    }

    public void __DRILL_INIT__()
        throws SchemaChangeException
    {
    }

    private final class BatchHolder
        extends HashTableTemplate.BatchHolder
    {

        IntVector vv0;
        IntVector vv4;
        IntVector vv9;
        IntVector vv13;
        IntVector vv16;
        IntVector vv20;

        public BatchHolder(int arg1) {
            super(arg1);
            try {
                __DRILL_INIT__();
            } catch (SchemaChangeException e) {
                throw new UnsupportedOperationException(e);
            }
        }

        public boolean isKeyMatchInternalBuild(int incomingRowIdx, int htRowIdx)
            throws SchemaChangeException
        {
            {
                IntHolder out3 = new IntHolder();
                {
                    out3 .value = vv0 .getAccessor().get((incomingRowIdx));
                }
                IntHolder out7 = new IntHolder();
                {
                    out7 .value = vv4 .getAccessor().get((htRowIdx));
                }
                //---- start of eval portion of compare_to_nulls_high function. ----//
                IntHolder out8 = new IntHolder();
                {
                    final IntHolder out = new IntHolder();
                    IntHolder left = out3;
                    IntHolder right = out7;
                     
GCompareIntVsInt$GCompareIntVsIntNullHigh_eval: {
    outside:
    {
        out.value = left.value < right.value ? -1 : (left.value == right.value ? 0 : 1);
    }
}
 
                    out8 = out;
                }
                //---- end of eval portion of compare_to_nulls_high function. ----//
                if (out8 .value!= 0) {
                    return false;
                }
                return true;
            }
        }

        public boolean isKeyMatchInternalProbe(int incomingRowIdx, int htRowIdx)
            throws SchemaChangeException
        {
            {
                return false;
            }
        }

        public void outputRecordKeys(int htRowIdx, int outRowIdx)
            throws SchemaChangeException
        {
            {
                IntHolder out19 = new IntHolder();
                {
                    out19 .value = vv16 .getAccessor().get((htRowIdx));
                }
                vv20 .getMutator().set((outRowIdx), out19 .value);
            }
        }

        public void setValue(int incomingRowIdx, int htRowIdx)
            throws SchemaChangeException
        {
            {
                IntHolder out12 = new IntHolder();
                {
                    out12 .value = vv9 .getAccessor().get((incomingRowIdx));
                }
                vv13 .getMutator().set((htRowIdx), out12 .value);
            }
        }

        public void setupInterior(RecordBatch incomingBuild, RecordBatch incomingProbe, RecordBatch outgoing, VectorContainer htContainer)
            throws SchemaChangeException
        {
            {
                int[] fieldIds1 = new int[ 1 ] ;
                fieldIds1 [ 0 ] = 0;
                Object tmp2 = (incomingBuild).getValueAccessorById(IntVector.class, fieldIds1).getValueVector();
                if (tmp2 == null) {
                    throw new SchemaChangeException("Failure while loading vector vv0 with id: TypedFieldId [fieldIds=[0], remainder=null].");
                }
                vv0 = ((IntVector) tmp2);
                int[] fieldIds5 = new int[ 1 ] ;
                fieldIds5 [ 0 ] = 0;
                Object tmp6 = (htContainer).getValueAccessorById(IntVector.class, fieldIds5).getValueVector();
                if (tmp6 == null) {
                    throw new SchemaChangeException("Failure while loading vector vv4 with id: TypedFieldId [fieldIds=[0], remainder=null].");
                }
                vv4 = ((IntVector) tmp6);
                /** start SETUP for function compare_to_nulls_high **/ 
                {
                     {}
                }
                /** end SETUP for function compare_to_nulls_high **/ 
                int[] fieldIds10 = new int[ 1 ] ;
                fieldIds10 [ 0 ] = 0;
                Object tmp11 = (incomingBuild).getValueAccessorById(IntVector.class, fieldIds10).getValueVector();
                if (tmp11 == null) {
                    throw new SchemaChangeException("Failure while loading vector vv9 with id: TypedFieldId [fieldIds=[0], remainder=null].");
                }
                vv9 = ((IntVector) tmp11);
                int[] fieldIds14 = new int[ 1 ] ;
                fieldIds14 [ 0 ] = 0;
                Object tmp15 = (htContainer).getValueAccessorById(IntVector.class, fieldIds14).getValueVector();
                if (tmp15 == null) {
                    throw new SchemaChangeException("Failure while loading vector vv13 with id: TypedFieldId [fieldIds=[0], remainder=null].");
                }
                vv13 = ((IntVector) tmp15);
            }
            {
                int[] fieldIds17 = new int[ 1 ] ;
                fieldIds17 [ 0 ] = 0;
                Object tmp18 = (htContainer).getValueAccessorById(IntVector.class, fieldIds17).getValueVector();
                if (tmp18 == null) {
                    throw new SchemaChangeException("Failure while loading vector vv16 with id: TypedFieldId [fieldIds=[0], remainder=null].");
                }
                vv16 = ((IntVector) tmp18);
                int[] fieldIds21 = new int[ 1 ] ;
                fieldIds21 [ 0 ] = 0;
                Object tmp22 = (outgoing).getValueAccessorById(IntVector.class, fieldIds21).getValueVector();
                if (tmp22 == null) {
                    throw new SchemaChangeException("Failure while loading vector vv20 with id: TypedFieldId [fieldIds=[0], remainder=null].");
                }
                vv20 = ((IntVector) tmp22);
            }
        }

        public void __DRILL_INIT__()
            throws SchemaChangeException
        {
        }

    }

}
