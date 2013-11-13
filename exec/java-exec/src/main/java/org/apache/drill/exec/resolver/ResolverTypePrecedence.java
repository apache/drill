package org.apache.drill.exec.resolver;

import java.util.HashMap;
import java.util.Map;

public class ResolverTypePrecedence {
	

public static final Map<String, Integer> precedenceMap;
    
    static {

   	 precedenceMap = new HashMap<String, Integer>();
   	 precedenceMap.put("NULLEXPRESSION", 0);	 
  	 precedenceMap.put("FIXEDBINARY", 1);
  	 precedenceMap.put("VARBINARY", 2);
   	 precedenceMap.put("VARCHAR", 3);
   	 precedenceMap.put("VAR16CHAR", 4);
   	 precedenceMap.put("FIXEDCHAR", 5);
   	 precedenceMap.put("FIXED16CHAR", 6);
   	 precedenceMap.put("BIT", 7);
   	 precedenceMap.put("TINYINT", 8);
   	 precedenceMap.put("SMALLINT", 9);
  	 precedenceMap.put("INT", 10);
  	 precedenceMap.put("BIGINT", 11);
  	 precedenceMap.put("MONEY", 12);
  	 precedenceMap.put("DECIMAL4", 13);
  	 precedenceMap.put("DECIMAL8", 14);
  	 precedenceMap.put("DECIMAL12", 15);
  	 precedenceMap.put("DECIMAL16", 16);
  	 precedenceMap.put("FLOAT4", 17);
  	 precedenceMap.put("FLOAT8", 18);
  	 precedenceMap.put("TIMETZ", 19);
  	 precedenceMap.put("TIME", 20);
  	 precedenceMap.put("DATE", 21);
  	 precedenceMap.put("DATETIME", 22);
  	 
    }


}
