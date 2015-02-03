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

import com.google.common.base.Joiner;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.lang.reflect.Field;
import java.net.URL;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.util.FileUtils;
import org.apache.drill.exec.expr.DrillFunc;
import org.apache.drill.exec.expr.DrillSimpleFunc;
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
import org.codehaus.commons.compiler.CompileException;
import org.codehaus.janino.Java.CompilationUnit;
import org.codehaus.janino.Parser;
import org.codehaus.janino.Scanner;
import org.mortbay.util.IO;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.InputSupplier;
import com.google.common.io.Resources;

/**
 * Converts FunctionCalls to Java Expressions.
 */
public class FunctionConverter {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FunctionConverter.class);

  private Map<String, CompilationUnit> functionUnits = Maps.newHashMap();

  private CompilationUnit get(Class<?> c) throws IOException{
    String path = c.getName();
    path = path.replaceFirst("\\$.*", "");
    path = path.replace(".", FileUtils.separator);
    path = "/" + path + ".java";
    CompilationUnit cu = functionUnits.get(path);
    if(cu != null) {
      return cu;
    }

    URL u = Resources.getResource(c, path);
    InputSupplier<InputStream> supplier = Resources.newInputStreamSupplier(u);
    try (InputStream is = supplier.getInput()) {
      if (is == null) {
        throw new IOException(String.format("Failure trying to located source code for Class %s, tried to read on classpath location %s", c.getName(), path));
      }
      String body = IO.toString(is);

      //TODO: Hack to remove annotations so Janino doesn't choke.  Need to reconsider this problem...
      body = body.replaceAll("@\\w+(?:\\([^\\\\]*?\\))?", "");
      try{
        cu = new Parser(new Scanner(null, new StringReader(body))).parseCompilationUnit();
        functionUnits.put(path, cu);
        return cu;
      } catch (CompileException e) {
        logger.warn("Failure while parsing function class:\n{}", body, e);
        return null;
      }

    }

  }

  /**
   * Get the name of the class used for interpreted expression evaluation.
   *
   * @return - class name of interpreted evaluator
   */
  private String getInterpreterClassName(Class clazz)  {
    return clazz.getName();
  }

  public <T extends DrillFunc> DrillFuncHolder getHolder(Class<T> clazz) {
    FunctionTemplate template = clazz.getAnnotation(FunctionTemplate.class);
    if (template == null) {
      return failure("Class does not declare FunctionTemplate annotation.", clazz);
    }

    if ((template.name().isEmpty() && template.names().length == 0) || // none set
        (!template.name().isEmpty() && template.names().length != 0)) { // both are set
      return failure("Must use only one annotations 'name' or 'names', not both", clazz);
    }

    // start by getting field information.
    List<ValueReference> params = Lists.newArrayList();
    List<WorkspaceReference> workspaceFields = Lists.newArrayList();

    ValueReference outputField = null;


    for (Field field : clazz.getDeclaredFields()) {

      Param param = field.getAnnotation(Param.class);
      Output output = field.getAnnotation(Output.class);
      Workspace workspace = field.getAnnotation(Workspace.class);
      Inject inject = field.getAnnotation(Inject.class);

      int i =0;
      if (param != null) {
        i++;
      }
      if (output != null) {
        i++;
      }
      if (workspace != null) {
        i++;
      }
      if (inject != null) {
        i++;
      }
      if (i == 0) {
        return failure("The field must be either a @Param, @Output, @Inject or @Workspace field.", clazz, field);
      } else if(i > 1) {
        return failure("The field must be only one of @Param, @Output, @Inject or @Workspace.  It currently has more than one of these annotations.", clazz, field);
      }

      if (param != null || output != null) {

        // Special processing for @Param FieldReader
        if (param != null && FieldReader.class.isAssignableFrom(field.getType())) {
          params.add(ValueReference.createFieldReaderRef(field.getName()));
          continue;
        }

        // Special processing for @Output ComplexWriter
        if (output != null && ComplexWriter.class.isAssignableFrom(field.getType())) {
          if (outputField != null) {
            return failure("You've declared more than one @Output field.  You must declare one and only @Output field per Function class.", clazz, field);
          }else{
            outputField = ValueReference.createComplexWriterRef(field.getName());
          }
          continue;
        }

        // check that param and output are value holders.
        if (!ValueHolder.class.isAssignableFrom(field.getType())) {
          return failure(String.format("The field doesn't holds value of type %s which does not implement the ValueHolder interface.  All fields of type @Param or @Output must extend this interface..", field.getType()), clazz, field);
        }

        // get the type field from the value holder.
        MajorType type = null;
        try {
          type = getStaticFieldValue("TYPE", field.getType(), MajorType.class);
        } catch (Exception e) {
          return failure("Failure while trying to access the ValueHolder's TYPE static variable.  All ValueHolders must contain a static TYPE variable that defines their MajorType.", e, clazz, field.getName());
        }


        ValueReference p = new ValueReference(type, field.getName());
        if (param != null) {
          if (param.constant()) {
            p.setConstant(true);
          }
          params.add(p);
        } else {
          if (outputField != null) {
            return failure("You've declared more than one @Output field.  You must declare one and only @Output field per Function class.", clazz, field);
          } else {
            outputField = p;
          }
        }

      } else {
        // workspace work.
        boolean isInject = inject != null;
        if (isInject && UdfUtilities.INJECTABLE_GETTER_METHODS.get(field.getType()) == null) {
          return failure(String.format("A %s cannot be injected into a %s, available injectable classes are: %s.",
              field.getType(), DrillFunc.class.getSimpleName(), Joiner.on(",").join(UdfUtilities.INJECTABLE_GETTER_METHODS.keySet())), clazz, field);
        }
        WorkspaceReference wsReference = new WorkspaceReference(field.getType(), field.getName(), isInject);

        if (!isInject && template.scope() == FunctionScope.POINT_AGGREGATE && !ValueHolder.class.isAssignableFrom(field.getType()) ) {
          return failure(String.format("Aggregate function '%s' workspace variable '%s' is of type '%s'. Please change it to Holder type.", template.name(), field.getName(), field.getType()), clazz, field);
        }

        //If the workspace var is of Holder type, get its MajorType and assign to WorkspaceReference.
        if (ValueHolder.class.isAssignableFrom(field.getType())) {
          MajorType majorType = null;
          try {
            majorType = getStaticFieldValue("TYPE", field.getType(), MajorType.class);
          } catch (Exception e) {
            return failure("Failure while trying to access the ValueHolder's TYPE static variable.  All ValueHolders must contain a static TYPE variable that defines their MajorType.", e, clazz, field.getName());
          }
          wsReference.setMajorType(majorType);
        }
        workspaceFields.add(wsReference);
      }
    }

   // if (!workspaceFields.isEmpty()) return failure("This function declares one or more workspace fields.  However, those have not yet been implemented.", clazz);
    if (outputField == null) {
      return failure("This function declares zero output fields.  A function must declare one output field.", clazz);
    }

    // get function body.

    CompilationUnit cu;
    try {
      cu = get(clazz);
      if (cu == null) {
        return null;
      }
    } catch (IOException e) {
      return failure("Failure while getting class body.", e, clazz);
    }


    try{
      Map<String, String> methods = MethodGrabbingVisitor.getMethods(cu, clazz);
      List<String> imports = ImportGrabber.getMethods(cu);
      // return holder
      ValueReference[] ps = params.toArray(new ValueReference[params.size()]);
      WorkspaceReference[] works = workspaceFields.toArray(new WorkspaceReference[workspaceFields.size()]);


      String[] registeredNames = ((template.name().isEmpty()) ? template.names() : new String[] {template.name()} );
      switch (template.scope()) {
      case POINT_AGGREGATE:
        return new DrillAggFuncHolder(template.scope(), template.nulls(), template.isBinaryCommutative(),
          template.isRandom(), registeredNames, ps, outputField, works, methods, imports, template.costCategory());
      case DECIMAL_AGGREGATE:
        return new DrillDecimalAggFuncHolder(template.scope(), template.nulls(), template.isBinaryCommutative(),
          template.isRandom(), registeredNames, ps, outputField, works, methods, imports);
      case DECIMAL_SUM_AGGREGATE:
        return new DrillDecimalSumAggFuncHolder(template.scope(), template.nulls(), template.isBinaryCommutative(),
          template.isRandom(), registeredNames, ps, outputField, works, methods, imports);
      case SIMPLE:
        if (outputField.isComplexWriter) {
          return new DrillComplexWriterFuncHolder(template.scope(), template.nulls(),
              template.isBinaryCommutative(),
              template.isRandom(), registeredNames,
              ps, outputField, works, methods, imports);
        } else {
          return new DrillSimpleFuncHolder(template.scope(), template.nulls(),
                                           template.isBinaryCommutative(),
                                           template.isRandom(), registeredNames,
                                           ps, outputField, works, methods, imports, template.costCategory(),
                                           (Class<DrillSimpleFunc>)clazz
                                           );
        }
      case SC_BOOLEAN_OPERATOR:
        return new DrillBooleanOPHolder(template.scope(), template.nulls(),
            template.isBinaryCommutative(),
            template.isRandom(), registeredNames,
            ps, outputField, works, methods, imports, FunctionTemplate.FunctionCostCategory.getDefault(), (Class<DrillSimpleFunc>)clazz);

      case DECIMAL_MAX_SCALE:
          return new DrillDecimalMaxScaleFuncHolder(template.scope(), template.nulls(), template.isBinaryCommutative(),
                  template.isRandom(), registeredNames, ps, outputField, works, methods, imports,
              FunctionTemplate.FunctionCostCategory.getDefault(), (Class<DrillSimpleFunc>)clazz);
      case DECIMAL_MUL_SCALE:
          return new DrillDecimalSumScaleFuncHolder(template.scope(), template.nulls(), template.isBinaryCommutative(),
                  template.isRandom(), registeredNames, ps, outputField, works, methods, imports,
              FunctionTemplate.FunctionCostCategory.getDefault(), (Class<DrillSimpleFunc>)clazz);
      case DECIMAL_ADD_SCALE:
          return new DrillDecimalAddFuncHolder(template.scope(), template.nulls(), template.isBinaryCommutative(),
                  template.isRandom(), registeredNames, ps, outputField, works, methods, imports,
              FunctionTemplate.FunctionCostCategory.getDefault(), (Class<DrillSimpleFunc>)clazz);
      case DECIMAL_CAST:
          return new DrillDecimalCastFuncHolder(template.scope(), template.nulls(), template.isBinaryCommutative(),
                  template.isRandom(), registeredNames, ps, outputField, works, methods, imports,
              FunctionTemplate.FunctionCostCategory.getDefault(), (Class<DrillSimpleFunc>)clazz);
      case DECIMAL_DIV_SCALE:
          return new DrillDecimalDivScaleFuncHolder(template.scope(), template.nulls(), template.isBinaryCommutative(),
                  template.isRandom(), registeredNames, ps, outputField, works, methods, imports,
              FunctionTemplate.FunctionCostCategory.getDefault(), (Class<DrillSimpleFunc>)clazz);
      case DECIMAL_MOD_SCALE:
          return new DrillDecimalModScaleFuncHolder(template.scope(), template.nulls(), template.isBinaryCommutative(),
                  template.isRandom(), registeredNames, ps, outputField, works, methods, imports,
              FunctionTemplate.FunctionCostCategory.getDefault(), (Class<DrillSimpleFunc>)clazz);
      case DECIMAL_SET_SCALE:
          return new DrillDecimalSetScaleFuncHolder(template.scope(), template.nulls(), template.isBinaryCommutative(),
                  template.isRandom(), registeredNames, ps, outputField, works, methods, imports,
              FunctionTemplate.FunctionCostCategory.getDefault(), (Class<DrillSimpleFunc>)clazz);
      case DECIMAL_ZERO_SCALE:
          return new DrillDecimalZeroScaleFuncHolder(template.scope(), template.nulls(), template.isBinaryCommutative(),
                  template.isRandom(), registeredNames, ps, outputField, works, methods, imports,
              FunctionTemplate.FunctionCostCategory.getDefault(), (Class<DrillSimpleFunc>)clazz);
      case HOLISTIC_AGGREGATE:
      case RANGE_AGGREGATE:
      default:
        return failure("Unsupported Function Type.", clazz);
      }
    } catch (Exception | NoSuchFieldError | AbstractMethodError ex) {
      return failure("Failure while creating function holder.", ex, clazz);
    }

  }



  private String getClassBody(Class<?> c) throws CompileException, IOException{
    String path = c.getName();
    path = path.replaceFirst("\\$.*", "");
    path = path.replace(".", FileUtils.separator);
    path = "/" + path + ".java";
    URL u = Resources.getResource(c, path);
    InputSupplier<InputStream> supplier = Resources.newInputStreamSupplier(u);
    try (InputStream is = supplier.getInput()) {
      if (is == null) {
        throw new IOException(String.format("Failure trying to located source code for Class %s, tried to read on classpath location %s", c.getName(), path));
      }
      String body = IO.toString(is);

      //TODO: Hack to remove annotations so Janino doesn't choke.  Need to reconsider this problem...
      //return body.replaceAll("@(?:Output|Param|Workspace|Override|SuppressWarnings\\([^\\\\]*?\\)|FunctionTemplate\\([^\\\\]*?\\))", "");
      return body.replaceAll("@(?:\\([^\\\\]*?\\))?", "");
    }

  }



  @SuppressWarnings("unchecked")
  private <T> T getStaticFieldValue(String fieldName, Class<?> valueType, Class<T> c) throws NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException{
      Field f = valueType.getDeclaredField(fieldName);
      Object val = f.get(null);
      return (T) val;
  }

  private static DrillFuncHolder failure(String message, Throwable t, Class<?> clazz, String fieldName) {
    logger.warn("Failure loading function class {}, field {}. " + message, clazz.getName(), fieldName, t);
    return null;
  }

  private DrillFuncHolder failure(String message, Class<?> clazz, String fieldName) {
    logger.warn("Failure loading function class {}, field {}. " + message, clazz.getName(), fieldName);
    return null;
  }

  private DrillFuncHolder failure(String message, Class<?> clazz) {
    logger.warn("Failure loading function class [{}]. Message: {}", clazz.getName(), message);
    return null;
  }

  private DrillFuncHolder failure(String message, Throwable t, Class<?> clazz) {
    logger.warn("Failure loading function class [{}]. Message: {}", clazz.getName(), message, t);
    return null;
  }

  private DrillFuncHolder failure(String message, Class<?> clazz, Field field) {
    return failure(message, clazz, field.getName());
  }

}
