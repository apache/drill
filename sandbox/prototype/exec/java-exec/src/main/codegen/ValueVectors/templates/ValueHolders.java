<@pp.dropOutputFile />
<#list modes as mode>
<#list types as type>
<#list type.minor as minor>

<#assign className="${mode.prefix}${minor.class}Holder" />
<@pp.changeOutputFile name="${className}.java" />
package org.apache.drill.exec.vector;

import org.apache.drill.common.types.Types;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.expr.holders.ValueHolder;
import io.netty.buffer.ByteBuf;

public final class ${className} implements ValueHolder{
  
  public static final MajorType TYPE = Types.${mode.name?lower_case}(MinorType.${minor.class?upper_case});

    <#if mode.name != "Repeated">
      
    public static final int WIDTH = ${type.width};
      <#if mode.name == "Optional">
      /** Whether the given holder holds a valid value.  1 means non-null.  0 means null. **/
      public int isSet;
      </#if>
      
      <#if type.major != "VarLen">
      
      <#if (type.width > 8)>
      public int start;
      public ByteBuf buffer;
      <#else>
        public ${minor.javaType!type.javaType} value;
      
      </#if>
      <#else>
      /** The first offset (inclusive) into the buffer. **/
      public int start;
      
      /** The last offset (exclusive) into the buffer. **/
      public int end;
      
      /** The buffer holding actual values. **/
      public ByteBuf buffer;
      </#if>
    <#else> 
    
      /** The first index (inclusive) into the Vector. **/
      public int start;
      
      /** The last index (exclusive) into the Vector. **/
      public int end;
      
      /** The Vector holding the actual values. **/
      public ${minor.class}Vector vector;
    </#if>
  
    
}

</#list>
</#list>
</#list>