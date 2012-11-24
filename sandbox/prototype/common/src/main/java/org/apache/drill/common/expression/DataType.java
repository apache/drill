package org.apache.drill.common.expression;

import java.util.Arrays;

public enum DataType{ // roughly taken from sql
	INVALID, BOOLEAN, BYTES, NVARCHAR, INTEGER, SMALLINT, VARBIT, FLOAT, DOUBLE, DATE, TIME, DATETIME, UNKNOWN;
	
	private DataType[] castable;
	DataType(DataType... castToDataTypes){
		castable = castToDataTypes;
		Arrays.sort(castable);
	}
	public boolean canCastTo(DataType dt){
		if(dt == this) return true;
		for(int i =0; i < castable.length; i++){
			if(dt.equals(castable[i])) return true;
		}
		return false;
	}
	
	public static DataType getCombinedCast(DataType a, DataType b){
		if(a == b) return a;
		
		if(a.canCastTo(b)) return b;
		if(b.canCastTo(a)) return a;
		return null;
	}
	
	
}