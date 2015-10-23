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
package org.apache.drill.exec.expr.fn;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.List;

import javax.inject.Inject;

import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.common.scanner.persistence.FieldDescriptor;
import org.apache.drill.common.scanner.persistence.AnnotatedClassDescriptor;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.exec.expr.DrillFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.FunctionTemplate.FunctionScope;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.annotations.Workspace;
import org.apache.drill.exec.expr.fn.DrillFuncHolder.ValueReference;
import org.apache.drill.exec.expr.fn.DrillFuncHolder.WorkspaceReference;
import org.apache.drill.exec.expr.holders.ValueHolder;
import org.apache.drill.exec.ops.UdfUtilities;
import org.apache.drill.exec.vector.complex.reader.FieldReader;
import org.apache.drill.exec.vector.complex.writer.BaseWriter.ComplexWriter;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;

/**
 * Converts FunctionCalls to Java Expressions.
 */
public class FunctionConverter {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FunctionConverter.class);

  public <T extends DrillFunc> DrillFuncHolder getHolder(AnnotatedClassDescriptor func) {
    FunctionTemplate template = func.getAnnotationProxy(FunctionTemplate.class);
    if (template == null) {
      return failure("Class does not declare FunctionTemplate annotation.", func);
    }

    String name = template.name();
    List<String> names = Arrays.asList(template.names());
    if (name.isEmpty() && names.isEmpty()) {
      // none set
      return failure("Must define 'name' or 'names'", func);
    }
    if (!name.isEmpty() && !names.isEmpty()) {
      // both are set
      return failure("Must use only one annotations 'name' or 'names', not both", func);
    }

    // start by getting field information.
    List<ValueReference> params = Lists.newArrayList();
    List<WorkspaceReference> workspaceFields = Lists.newArrayList();

    ValueReference outputField = null;

    for (FieldDescriptor field : func.getFields()) {
      Param param = field.getAnnotationProxy(Param.class);
      Output output = field.getAnnotationProxy(Output.class);
      Workspace workspace = field.getAnnotationProxy(Workspace.class);
      Inject inject = field.getAnnotationProxy(Inject.class);

      Annotation[] annotations = {param, output, workspace, inject};
      int annotationCount = 0;
      for (Annotation annotationDescriptor : annotations) {
        if (annotationDescriptor != null) {
          annotationCount += 1;
        }
      }
      if (annotationCount == 0) {
        return failure("The field must be either a @Param, @Output, @Inject or @Workspace field.", func, field);
      } else if(annotationCount > 1) {
        return failure("The field must be only one of @Param, @Output, @Inject or @Workspace. It currently has more than one of these annotations.", func, field);
      }

      // TODO(Julien): verify there are a few of those and we can load them
      Class<?> fieldClass = field.getFieldClass();
      if (param != null || output != null) {

        // Special processing for @Param FieldReader
        if (param != null && FieldReader.class.isAssignableFrom(fieldClass)) {
          params.add(ValueReference.createFieldReaderRef(field.getName()));
          continue;
        }

        // Special processing for @Output ComplexWriter
        if (output != null && ComplexWriter.class.isAssignableFrom(fieldClass)) {
          if (outputField != null) {
            return failure("You've declared more than one @Output field.  You must declare one and only @Output field per Function class.", func, field);
          }else{
            outputField = ValueReference.createComplexWriterRef(field.getName());
          }
          continue;
        }

        // check that param and output are value holders.
        if (!ValueHolder.class.isAssignableFrom(fieldClass)) {
          return failure(String.format("The field doesn't holds value of type %s which does not implement the ValueHolder interface.  All fields of type @Param or @Output must extend this interface..", fieldClass), func, field);
        }

        // get the type field from the value holder.
        MajorType type = null;
        try {
          type = getStaticFieldValue("TYPE", fieldClass, MajorType.class);
        } catch (Exception e) {
          return failure("Failure while trying to access the ValueHolder's TYPE static variable.  All ValueHolders must contain a static TYPE variable that defines their MajorType.", e, func, field);
        }

        ValueReference p = new ValueReference(type, field.getName());
        if (param != null) {
          p.setConstant(param.constant());
          params.add(p);
        } else {
          if (outputField != null) {
            return failure("You've declared more than one @Output field.  You must declare one and only @Output field per Function class.", func, field);
          } else {
            outputField = p;
          }
        }

      } else {
        // workspace work.
        boolean isInject = inject != null;
        if (isInject && UdfUtilities.INJECTABLE_GETTER_METHODS.get(fieldClass) == null) {
          return failure(
              String.format(
                  "A %s cannot be injected into a %s,"
                  + " available injectable classes are: %s.",
                  fieldClass, DrillFunc.class.getSimpleName(),
                  Joiner.on(",").join(UdfUtilities.INJECTABLE_GETTER_METHODS.keySet())
              ), func, field);
        }
        WorkspaceReference wsReference = new WorkspaceReference(fieldClass, field.getName(), isInject);

        if (!isInject && template.scope() == FunctionScope.POINT_AGGREGATE && !ValueHolder.class.isAssignableFrom(fieldClass) ) {
          return failure(String.format("Aggregate function '%s' workspace variable '%s' is of type '%s'. Please change it to Holder type.", func.getClassName(), field.getName(), fieldClass), func, field);
        }

        //If the workspace var is of Holder type, get its MajorType and assign to WorkspaceReference.
        if (ValueHolder.class.isAssignableFrom(fieldClass)) {
          MajorType majorType = null;
          try {
            majorType = getStaticFieldValue("TYPE", fieldClass, MajorType.class);
          } catch (Exception e) {
            return failure("Failure while trying to access the ValueHolder's TYPE static variable.  All ValueHolders must contain a static TYPE variable that defines their MajorType.", e, func, field);
          }
          wsReference.setMajorType(majorType);
        }
        workspaceFields.add(wsReference);
      }
    }

    if (outputField == null) {
      return failure("This function declares zero output fields.  A function must declare one output field.", func);
    }

    FunctionInitializer initializer = new FunctionInitializer(func.getClassName());
    try{
      // return holder
      ValueReference[] ps = params.toArray(new ValueReference[params.size()]);
      WorkspaceReference[] works = workspaceFields.toArray(new WorkspaceReference[workspaceFields.size()]);


      String[] registeredNames = ((template.name().isEmpty()) ? template.names() : new String[] {template.name()} );
      boolean isDeteministic = ! template.isRandom();
      FunctionAttributes functionAttributes = new FunctionAttributes(
          template.scope(),
          template.nulls(),
          template.isBinaryCommutative(),
          isDeteministic, registeredNames, ps, outputField, works, template.costCategory());
      switch (template.scope()) {
      case POINT_AGGREGATE:
        return new DrillAggFuncHolder(functionAttributes, initializer);
      case DECIMAL_AGGREGATE:
        return new DrillDecimalAggFuncHolder(functionAttributes, initializer);
      case DECIMAL_SUM_AGGREGATE:
        return new DrillDecimalSumAggFuncHolder(functionAttributes, initializer);
      case SIMPLE:
        if (outputField.isComplexWriter) {
          return new DrillComplexWriterFuncHolder(functionAttributes, initializer);
        } else {
          return new DrillSimpleFuncHolder(functionAttributes, initializer);
        }
      case SC_BOOLEAN_OPERATOR:
        return new DrillBooleanOPHolder(functionAttributes, initializer);
      case DECIMAL_MAX_SCALE:
          return new DrillDecimalMaxScaleFuncHolder(functionAttributes, initializer);
      case DECIMAL_MUL_SCALE:
          return new DrillDecimalSumScaleFuncHolder(functionAttributes, initializer);
      case DECIMAL_ADD_SCALE:
          return new DrillDecimalAddFuncHolder(functionAttributes, initializer);
      case DECIMAL_CAST:
          return new DrillDecimalCastFuncHolder(functionAttributes, initializer);
      case DECIMAL_DIV_SCALE:
          return new DrillDecimalDivScaleFuncHolder(functionAttributes, initializer);
      case DECIMAL_MOD_SCALE:
          return new DrillDecimalModScaleFuncHolder(functionAttributes, initializer);
      case DECIMAL_SET_SCALE:
          return new DrillDecimalSetScaleFuncHolder(functionAttributes, initializer);
      case DECIMAL_ZERO_SCALE:
          return new DrillDecimalZeroScaleFuncHolder(functionAttributes, initializer);
      case HOLISTIC_AGGREGATE:
      case RANGE_AGGREGATE:
      default:
        return failure("Unsupported Function Type.", func);
      }
    } catch (Exception | NoSuchFieldError | AbstractMethodError ex) {
      return failure("Failure while creating function holder.", ex, func);
    }
  }

  @SuppressWarnings("unchecked")
  private <T> T getStaticFieldValue(String fieldName, Class<?> valueType, Class<T> c) throws NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException{
      Field f = valueType.getDeclaredField(fieldName);
      Object val = f.get(null);
      return (T) val;
  }

  private static DrillFuncHolder failure(String message, Throwable t, AnnotatedClassDescriptor func, FieldDescriptor field) {
    return fieldFailure(message, t, func.getClassName(), field.getName());
  }

  private static DrillFuncHolder failure(String message, AnnotatedClassDescriptor func, FieldDescriptor field) {
    return fieldFailure(message, null, func.getClassName(), field.getName());
  }

  private DrillFuncHolder failure(String message, AnnotatedClassDescriptor func) {
    return classFailure(message, null, func.getClassName());
  }

  private DrillFuncHolder failure(String message, Throwable t, AnnotatedClassDescriptor func) {
    return classFailure(message, t, func.getClassName());
  }

  private static DrillFuncHolder classFailure(String message, Throwable t, String funcClassName) {
    return failure(String.format("Failure loading function class [%s]. Message: %s", funcClassName, message), t);
  }

  private static DrillFuncHolder fieldFailure(String message, Throwable t, String funcClassName, String fieldName) {
    return failure(String.format("Failure loading function class %s, field %s. Message: %s", funcClassName, fieldName, message), t);
  }

  private static DrillFuncHolder failure(String message, Throwable t) {
    if (t == null) {
      t = new DrillRuntimeException(message);
    }
    logger.warn(message, t);
    return null;
  }

}
