package org.apache.drill.exec.resolver;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.drill.common.types.TypeProtos.MinorType;

public class TypeCastRules {
	
	private static Map<MinorType, Set<MinorType>> rules;
	
	public TypeCastRules(){
		
		
		rules = new HashMap<MinorType, Set<MinorType>>();

        Set<MinorType> rule;

        /** TINYINT cast able from **/
        rule = new HashSet<MinorType>();
        rule.add(MinorType.TINYINT);        
        rule.add(MinorType.SMALLINT);        
        rule.add(MinorType.INT);        
        rule.add(MinorType.BIGINT);        
        rule.add(MinorType.DECIMAL4);        
        rule.add(MinorType.DECIMAL8);        
        rule.add(MinorType.DECIMAL12);        
        rule.add(MinorType.DECIMAL16);        
        rule.add(MinorType.MONEY);        
        rule.add(MinorType.FLOAT4);        
        rule.add(MinorType.FLOAT8);        
        rule.add(MinorType.BIT);        
        rule.add(MinorType.FIXEDCHAR);        
        rule.add(MinorType.FIXED16CHAR);        
        rule.add(MinorType.FIXEDBINARY);        
        rule.add(MinorType.VARCHAR);        
        rule.add(MinorType.VAR16CHAR);        
        rule.add(MinorType.VARBINARY);        
        rules.put(MinorType.TINYINT, rule);
        
        /** SMALLINT cast able from **/
        rule = new HashSet<MinorType>();
        rule.add(MinorType.TINYINT);        
        rule.add(MinorType.SMALLINT);        
        rule.add(MinorType.INT);        
        rule.add(MinorType.BIGINT);        
        rule.add(MinorType.DECIMAL4);        
        rule.add(MinorType.DECIMAL8);        
        rule.add(MinorType.DECIMAL12);        
        rule.add(MinorType.DECIMAL16);        
        rule.add(MinorType.MONEY);        
        rule.add(MinorType.FLOAT4);        
        rule.add(MinorType.FLOAT8);        
        rule.add(MinorType.BIT);        
        rule.add(MinorType.FIXEDCHAR);        
        rule.add(MinorType.FIXED16CHAR);        
        rule.add(MinorType.FIXEDBINARY);        
        rule.add(MinorType.VARCHAR);        
        rule.add(MinorType.VAR16CHAR);        
        rule.add(MinorType.VARBINARY);        
        rules.put(MinorType.SMALLINT, rule);
        
        /** INT cast able from **/
        rule = new HashSet<MinorType>();
        rule.add(MinorType.TINYINT);        
        rule.add(MinorType.SMALLINT);        
        rule.add(MinorType.INT);        
        rule.add(MinorType.BIGINT);        
        rule.add(MinorType.DECIMAL4);        
        rule.add(MinorType.DECIMAL8);        
        rule.add(MinorType.DECIMAL12);        
        rule.add(MinorType.DECIMAL16);        
        rule.add(MinorType.MONEY);        
        rule.add(MinorType.FLOAT4);        
        rule.add(MinorType.FLOAT8);        
        rule.add(MinorType.BIT);        
        rule.add(MinorType.FIXEDCHAR);        
        rule.add(MinorType.FIXED16CHAR);        
        rule.add(MinorType.FIXEDBINARY);        
        rule.add(MinorType.VARCHAR);        
        rule.add(MinorType.VAR16CHAR);        
        rule.add(MinorType.VARBINARY);        
        rules.put(MinorType.INT, rule);
        
        /** BIGINT cast able from **/
        rule = new HashSet<MinorType>();
        rule.add(MinorType.TINYINT);        
        rule.add(MinorType.SMALLINT);        
        rule.add(MinorType.INT);        
        rule.add(MinorType.BIGINT);        
        rule.add(MinorType.DECIMAL4);        
        rule.add(MinorType.DECIMAL8);        
        rule.add(MinorType.DECIMAL12);        
        rule.add(MinorType.DECIMAL16);        
        rule.add(MinorType.MONEY);        
        rule.add(MinorType.FLOAT4);        
        rule.add(MinorType.FLOAT8);        
        rule.add(MinorType.BIT);        
        rule.add(MinorType.FIXEDCHAR);        
        rule.add(MinorType.FIXED16CHAR);        
        rule.add(MinorType.FIXEDBINARY);        
        rule.add(MinorType.VARCHAR);        
        rule.add(MinorType.VAR16CHAR);        
        rule.add(MinorType.VARBINARY);        
        rules.put(MinorType.BIGINT, rule);
               
        /** DECIMAL4 cast able from **/
        rule = new HashSet<MinorType>();
        rule.add(MinorType.TINYINT);        
        rule.add(MinorType.SMALLINT);        
        rule.add(MinorType.INT);        
        rule.add(MinorType.BIGINT);        
        rule.add(MinorType.DECIMAL4);      
        rule.add(MinorType.MONEY);        
        rule.add(MinorType.FLOAT4);        
        rule.add(MinorType.FLOAT8);        
        rule.add(MinorType.BIT);        
        rule.add(MinorType.FIXEDCHAR);        
        rule.add(MinorType.FIXED16CHAR);        
        rule.add(MinorType.FIXEDBINARY);        
        rule.add(MinorType.VARCHAR);        
        rule.add(MinorType.VAR16CHAR);        
        rule.add(MinorType.VARBINARY);        
        rules.put(MinorType.DECIMAL4, rule);
        
        /** DECIMAL8 cast able from **/
        rule = new HashSet<MinorType>();
        rule.add(MinorType.TINYINT);        
        rule.add(MinorType.SMALLINT);        
        rule.add(MinorType.INT);        
        rule.add(MinorType.BIGINT);        
        rule.add(MinorType.DECIMAL4);        
        rule.add(MinorType.DECIMAL8);       
        rule.add(MinorType.MONEY);        
        rule.add(MinorType.FLOAT4);        
        rule.add(MinorType.FLOAT8);        
        rule.add(MinorType.BIT);        
        rule.add(MinorType.FIXEDCHAR);        
        rule.add(MinorType.FIXED16CHAR);        
        rule.add(MinorType.FIXEDBINARY);        
        rule.add(MinorType.VARCHAR);        
        rule.add(MinorType.VAR16CHAR);        
        rule.add(MinorType.VARBINARY);        
        rules.put(MinorType.DECIMAL8, rule);
        
        /** DECIMAL12 cast able from **/
        rule = new HashSet<MinorType>();
        rule.add(MinorType.TINYINT);        
        rule.add(MinorType.SMALLINT);        
        rule.add(MinorType.INT);        
        rule.add(MinorType.BIGINT);        
        rule.add(MinorType.DECIMAL4);        
        rule.add(MinorType.DECIMAL8);        
        rule.add(MinorType.DECIMAL12);        
        rule.add(MinorType.MONEY);        
        rule.add(MinorType.FLOAT4);        
        rule.add(MinorType.FLOAT8);        
        rule.add(MinorType.BIT);        
        rule.add(MinorType.FIXEDCHAR);        
        rule.add(MinorType.FIXED16CHAR);        
        rule.add(MinorType.FIXEDBINARY);        
        rule.add(MinorType.VARCHAR);        
        rule.add(MinorType.VAR16CHAR);        
        rule.add(MinorType.VARBINARY);        
        rules.put(MinorType.DECIMAL12, rule);
        
        /** DECIMAL16 cast able from **/
        rule = new HashSet<MinorType>();
        rule.add(MinorType.TINYINT);        
        rule.add(MinorType.SMALLINT);        
        rule.add(MinorType.INT);        
        rule.add(MinorType.BIGINT);        
        rule.add(MinorType.DECIMAL4);        
        rule.add(MinorType.DECIMAL8);        
        rule.add(MinorType.DECIMAL12);        
        rule.add(MinorType.DECIMAL16);        
        rule.add(MinorType.MONEY);        
        rule.add(MinorType.FLOAT4);        
        rule.add(MinorType.FLOAT8);        
        rule.add(MinorType.BIT);        
        rule.add(MinorType.FIXEDCHAR);        
        rule.add(MinorType.FIXED16CHAR);        
        rule.add(MinorType.FIXEDBINARY);        
        rule.add(MinorType.VARCHAR);        
        rule.add(MinorType.VAR16CHAR);        
        rule.add(MinorType.VARBINARY);        
        rules.put(MinorType.DECIMAL16, rule);
        
        /** MONEY cast able from **/
        rule = new HashSet<MinorType>();
        rule.add(MinorType.TINYINT);        
        rule.add(MinorType.SMALLINT);        
        rule.add(MinorType.INT);        
        rule.add(MinorType.BIGINT);        
        rule.add(MinorType.DECIMAL4);        
        rule.add(MinorType.DECIMAL8);        
        rule.add(MinorType.DECIMAL12);        
        rule.add(MinorType.DECIMAL16);        
        rule.add(MinorType.MONEY);        
        rule.add(MinorType.FLOAT4);        
        rule.add(MinorType.FLOAT8);        
        rule.add(MinorType.BIT);        
        rule.add(MinorType.FIXEDCHAR);        
        rule.add(MinorType.FIXED16CHAR);        
        rule.add(MinorType.FIXEDBINARY);        
        rule.add(MinorType.VARCHAR);        
        rule.add(MinorType.VAR16CHAR);        
        rule.add(MinorType.VARBINARY);        
        rules.put(MinorType.MONEY, rule);
        
        /** DATE cast able from **/
        rule = new HashSet<MinorType>();
        rule.add(MinorType.DATE);        
        rule.add(MinorType.DATETIME);        
        rule.add(MinorType.FIXEDCHAR);        
        rule.add(MinorType.FIXED16CHAR);        
        rule.add(MinorType.FIXEDBINARY);        
        rule.add(MinorType.VARCHAR);        
        rule.add(MinorType.VAR16CHAR);        
        rule.add(MinorType.VARBINARY);
        rules.put(MinorType.DATE, rule);
        
        /** TIME cast able from **/
        rule = new HashSet<MinorType>();
        rule.add(MinorType.TIME);        
        rule.add(MinorType.DATETIME);        
        rule.add(MinorType.FIXEDCHAR);        
        rule.add(MinorType.FIXED16CHAR);        
        rule.add(MinorType.FIXEDBINARY);        
        rule.add(MinorType.VARCHAR);        
        rule.add(MinorType.VAR16CHAR);        
        rule.add(MinorType.VARBINARY);
        rules.put(MinorType.TIME, rule);
        
        /** DATETIME cast able from **/
        rule = new HashSet<MinorType>();
        rule.add(MinorType.DATETIME);        
        rule.add(MinorType.TINYINT);        
        rule.add(MinorType.SMALLINT);        
        rule.add(MinorType.INT);        
        rule.add(MinorType.BIGINT);        
        rule.add(MinorType.DECIMAL4);        
        rule.add(MinorType.DECIMAL8);        
        rule.add(MinorType.DECIMAL12);        
        rule.add(MinorType.DECIMAL16);
        rule.add(MinorType.DATE);
        rule.add(MinorType.TIME);
        rule.add(MinorType.TIMESTAMP);
        rules.put(MinorType.DATETIME, rule);
        
        /** FLOAT4 cast able from **/
        rule = new HashSet<MinorType>();
        rule.add(MinorType.TINYINT);        
        rule.add(MinorType.SMALLINT);        
        rule.add(MinorType.INT);        
        rule.add(MinorType.BIGINT);        
        rule.add(MinorType.DECIMAL4);        
        rule.add(MinorType.DECIMAL8);        
        rule.add(MinorType.DECIMAL12);        
        rule.add(MinorType.DECIMAL16);        
        rule.add(MinorType.MONEY);        
        rule.add(MinorType.FLOAT4);        
        rule.add(MinorType.BIT);        
        rule.add(MinorType.FIXEDCHAR);        
        rule.add(MinorType.FIXED16CHAR);        
        rule.add(MinorType.VARCHAR);        
        rule.add(MinorType.VAR16CHAR);        
        rules.put(MinorType.FLOAT4, rule);
        
        /** FLOAT8 cast able from **/
        rule = new HashSet<MinorType>();
        rule.add(MinorType.TINYINT);        
        rule.add(MinorType.SMALLINT);        
        rule.add(MinorType.INT);        
        rule.add(MinorType.BIGINT);        
        rule.add(MinorType.DECIMAL4);        
        rule.add(MinorType.DECIMAL8);        
        rule.add(MinorType.DECIMAL12);        
        rule.add(MinorType.DECIMAL16);        
        rule.add(MinorType.MONEY);        
        rule.add(MinorType.FLOAT4);        
        rule.add(MinorType.FLOAT8);        
        rule.add(MinorType.BIT);        
        rule.add(MinorType.FIXEDCHAR);        
        rule.add(MinorType.FIXED16CHAR);        
        rule.add(MinorType.VARCHAR);        
        rule.add(MinorType.VAR16CHAR);        
        rules.put(MinorType.FLOAT8, rule);
        
        /** BIT cast able from **/
        rule = new HashSet<MinorType>();
        rule.add(MinorType.TINYINT);        
        rule.add(MinorType.SMALLINT);        
        rule.add(MinorType.INT);        
        rule.add(MinorType.BIGINT);        
        rule.add(MinorType.DECIMAL4);        
        rule.add(MinorType.DECIMAL8);        
        rule.add(MinorType.DECIMAL12);        
        rule.add(MinorType.DECIMAL16);        
        rule.add(MinorType.MONEY);        
        rule.add(MinorType.TIMESTAMP);        
        rule.add(MinorType.FLOAT4);        
        rule.add(MinorType.FLOAT8);        
        rule.add(MinorType.BIT);        
        rule.add(MinorType.FIXEDCHAR);        
        rule.add(MinorType.FIXED16CHAR);        
        rule.add(MinorType.VARCHAR);        
        rule.add(MinorType.VAR16CHAR);        
        rule.add(MinorType.VARBINARY);        
        rule.add(MinorType.FIXEDBINARY);        
        rules.put(MinorType.BIT, rule);
        
        /** FIXEDCHAR cast able from **/
        rule = new HashSet<MinorType>();
        rule.add(MinorType.TINYINT);        
        rule.add(MinorType.SMALLINT);        
        rule.add(MinorType.INT);        
        rule.add(MinorType.BIGINT);        
        rule.add(MinorType.DECIMAL4);        
        rule.add(MinorType.DECIMAL8);        
        rule.add(MinorType.DECIMAL12);        
        rule.add(MinorType.DECIMAL16);        
        rule.add(MinorType.MONEY);        
        rule.add(MinorType.TIMESTAMP);        
        rule.add(MinorType.FLOAT4);        
        rule.add(MinorType.FLOAT8);        
        rule.add(MinorType.BIT);        
        rule.add(MinorType.FIXEDCHAR);        
        rule.add(MinorType.FIXED16CHAR);        
        rule.add(MinorType.VARCHAR);        
        rule.add(MinorType.VAR16CHAR);        
        rule.add(MinorType.VARBINARY);        
        rule.add(MinorType.FIXEDBINARY);        
        rule.add(MinorType.DATE);        
        rule.add(MinorType.TIME);        
        rule.add(MinorType.DATETIME);        
        rules.put(MinorType.FIXEDCHAR, rule);
        
        /** FIXED16CHAR cast able from **/
        rule = new HashSet<MinorType>();
        rule.add(MinorType.TINYINT);        
        rule.add(MinorType.SMALLINT);        
        rule.add(MinorType.INT);        
        rule.add(MinorType.BIGINT);        
        rule.add(MinorType.DECIMAL4);        
        rule.add(MinorType.DECIMAL8);        
        rule.add(MinorType.DECIMAL12);        
        rule.add(MinorType.DECIMAL16);        
        rule.add(MinorType.MONEY);        
        rule.add(MinorType.TIMESTAMP);        
        rule.add(MinorType.FLOAT4);        
        rule.add(MinorType.FLOAT8);        
        rule.add(MinorType.BIT);        
        rule.add(MinorType.FIXEDCHAR);        
        rule.add(MinorType.FIXED16CHAR);        
        rule.add(MinorType.VARCHAR);        
        rule.add(MinorType.VAR16CHAR);        
        rule.add(MinorType.VARBINARY);        
        rule.add(MinorType.FIXEDBINARY);        
        rule.add(MinorType.DATE);        
        rule.add(MinorType.TIME);        
        rule.add(MinorType.DATETIME);        
        rules.put(MinorType.FIXED16CHAR, rule);
        
        /** FIXEDBINARY cast able from **/
        rule = new HashSet<MinorType>();
        rule.add(MinorType.TINYINT);        
        rule.add(MinorType.SMALLINT);        
        rule.add(MinorType.INT);        
        rule.add(MinorType.BIGINT);        
        rule.add(MinorType.DECIMAL4);        
        rule.add(MinorType.DECIMAL8);        
        rule.add(MinorType.DECIMAL12);        
        rule.add(MinorType.DECIMAL16);        
        rule.add(MinorType.MONEY);        
        rule.add(MinorType.TIMESTAMP);        
        rule.add(MinorType.FLOAT4);        
        rule.add(MinorType.FLOAT8);        
        rule.add(MinorType.BIT);        
        rule.add(MinorType.VARCHAR);        
        rule.add(MinorType.VAR16CHAR);        
        rule.add(MinorType.VARBINARY);        
        rule.add(MinorType.FIXEDBINARY);        
        rules.put(MinorType.FIXEDBINARY, rule);
        
        /** VARCHAR cast able from **/
        rule = new HashSet<MinorType>();
        rule.add(MinorType.TINYINT);        
        rule.add(MinorType.SMALLINT);        
        rule.add(MinorType.INT);        
        rule.add(MinorType.BIGINT);        
        rule.add(MinorType.DECIMAL4);        
        rule.add(MinorType.DECIMAL8);        
        rule.add(MinorType.DECIMAL12);        
        rule.add(MinorType.DECIMAL16);        
        rule.add(MinorType.MONEY);        
        rule.add(MinorType.TIMESTAMP);        
        rule.add(MinorType.FLOAT4);        
        rule.add(MinorType.FLOAT8);        
        rule.add(MinorType.BIT);        
        rule.add(MinorType.FIXEDCHAR);        
        rule.add(MinorType.FIXED16CHAR);        
        rule.add(MinorType.VARCHAR);        
        rule.add(MinorType.VAR16CHAR);        
        rule.add(MinorType.VARBINARY);        
        rule.add(MinorType.FIXEDBINARY);        
        rule.add(MinorType.DATE);        
        rule.add(MinorType.TIME);        
        rule.add(MinorType.DATETIME);        
        rules.put(MinorType.VARCHAR, rule);
        
        /** VAR16CHAR cast able from **/
        rule = new HashSet<MinorType>();
        rule.add(MinorType.TINYINT);        
        rule.add(MinorType.SMALLINT);        
        rule.add(MinorType.INT);        
        rule.add(MinorType.BIGINT);        
        rule.add(MinorType.DECIMAL4);        
        rule.add(MinorType.DECIMAL8);        
        rule.add(MinorType.DECIMAL12);        
        rule.add(MinorType.DECIMAL16);        
        rule.add(MinorType.MONEY);        
        rule.add(MinorType.TIMESTAMP);        
        rule.add(MinorType.FLOAT4);        
        rule.add(MinorType.FLOAT8);        
        rule.add(MinorType.BIT);        
        rule.add(MinorType.FIXEDCHAR);        
        rule.add(MinorType.FIXED16CHAR);        
        rule.add(MinorType.VARCHAR);        
        rule.add(MinorType.VAR16CHAR);        
        rule.add(MinorType.VARBINARY);        
        rule.add(MinorType.FIXEDBINARY);        
        rule.add(MinorType.DATE);        
        rule.add(MinorType.TIME);        
        rule.add(MinorType.DATETIME);        
        rules.put(MinorType.VAR16CHAR, rule);
        
        /** VARBINARY cast able from **/
        rule = new HashSet<MinorType>();
        rule.add(MinorType.TINYINT);        
        rule.add(MinorType.SMALLINT);        
        rule.add(MinorType.INT);        
        rule.add(MinorType.BIGINT);        
        rule.add(MinorType.DECIMAL4);        
        rule.add(MinorType.DECIMAL8);        
        rule.add(MinorType.DECIMAL12);        
        rule.add(MinorType.DECIMAL16);        
        rule.add(MinorType.MONEY);        
        rule.add(MinorType.TIMESTAMP);        
        rule.add(MinorType.FLOAT4);        
        rule.add(MinorType.FLOAT8);        
        rule.add(MinorType.BIT);        
        rule.add(MinorType.VARBINARY);        
        rule.add(MinorType.FIXEDBINARY);        
        rules.put(MinorType.VARBINARY, rule);
        
        
	}
	
	public static boolean isCastable(MinorType from, MinorType to){		
		return rules.get(from)==null ? false: rules.get(from).contains(to);
	}

}
