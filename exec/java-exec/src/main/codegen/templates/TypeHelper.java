<@pp.dropOutputFile />
<@pp.changeOutputFile name="/org/apache/drill/exec/expr/TypeHelper.java" />

<#include "/@includes/license.ftl" />

package org.apache.drill.exec.expr;

<#include "/@includes/vv_imports.ftl" />


public class TypeHelper {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TypeHelper.class);

  private static final int WIDTH_ESTIMATE = 50;

  public static int getSize(MajorType major) {
    switch (major.getMinorType()) {
<#list vv.types as type>
  <#list type.minor as minor>
    case ${minor.class?upper_case}:
      return ${type.width}<#if minor.class?substring(0, 3) == "Var" ||
                               minor.class?substring(0, 3) == "PRO" ||
                               minor.class?substring(0, 3) == "MSG"> + WIDTH_ESTIMATE</#if>;
  </#list>
</#list>
      case FIXEDCHAR: return major.getWidth();
      case FIXED16CHAR: return major.getWidth();
      case FIXEDBINARY: return major.getWidth();
    }
    throw new UnsupportedOperationException();
  }

  public static Class<?> getValueVectorClass(MinorType type, DataMode mode){
    switch (type) {
<#list vv.types as type>
  <#list type.minor as minor>
      case ${minor.class?upper_case}:
        switch (mode) {
          case REQUIRED:
            return ${minor.class}Vector.class;
          case OPTIONAL:
            return Nullable${minor.class}Vector.class;
          case REPEATED:
            return Repeated${minor.class}Vector.class;
        }
  </#list>
</#list>
    default:
      break;
    }
    throw new UnsupportedOperationException();
  }

  public static JType getHolderType(JCodeModel model, MinorType type, DataMode mode){
    switch (type) {
<#list vv.types as type>
  <#list type.minor as minor>
      case ${minor.class?upper_case}:
        switch (mode) {
          case REQUIRED:
            return model._ref(${minor.class}Holder.class);
          case OPTIONAL:
            return model._ref(Nullable${minor.class}Holder.class);
          case REPEATED:
            return model._ref(Repeated${minor.class}Holder.class);
        }
  </#list>
</#list>
      default:
        break;
      }
      throw new UnsupportedOperationException();
  }

  public static ValueVector getNewVector(MaterializedField field, BufferAllocator allocator){
    MajorType type = field.getType();

    switch (type.getMinorType()) {
<#list vv.  types as type>
  <#list type.minor as minor>
    case ${minor.class?upper_case}:
      switch (type.getMode()) {
        case REQUIRED:
          return new ${minor.class}Vector(field, allocator);
        case OPTIONAL:
          return new Nullable${minor.class}Vector(field, allocator);
        case REPEATED:
          return new Repeated${minor.class}Vector(field, allocator);
      }
  </#list>
</#list>
    default:
      break;
    }
    // All ValueVector types have been handled.
    throw new UnsupportedOperationException(type.getMinorType() + " type is not supported. Mode: " + type.getMode());
  }

}
