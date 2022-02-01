package org.apache.calcite.sql.validate;

import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.DynamicRootSchema;
import org.apache.calcite.jdbc.DynamicSchema;
import org.apache.drill.exec.rpc.user.UserSession;

public class DrillSqlValidatorUtil {

  /**
   * Finds and returns {@link CalciteSchema} nested to the given rootSchema
   * with specified schemaPath.
   *
   * <p>Uses the case-sensitivity policy of specified nameMatcher.
   *
   * <p>If not found, returns null.
   *
   * @param rootSchema root schema
   * @param schemaPath full schema path of required schema
   * @param nameMatcher name matcher
   * @param session Drill user session
   *
   * @return CalciteSchema that corresponds specified schemaPath
   */
  public static CalciteSchema getSchema(CalciteSchema rootSchema,
                                        Iterable<String> schemaPath, SqlNameMatcher nameMatcher, UserSession session) {
    CalciteSchema schema = rootSchema;
    DrillSqlValidatorUtil.setUserSession(schema, session);
    for (String schemaName : schemaPath) {
      if (schema == rootSchema
        && nameMatcher.matches(schemaName, schema.getName())) {
        DrillSqlValidatorUtil.setUserSession(schema, session);
        continue;
      }
      schema = schema.getSubSchema(schemaName,
        nameMatcher.isCaseSensitive());
      if (schema == null) {
        return null;
      }

      DrillSqlValidatorUtil.setUserSession(schema, session);
    }
    return schema;
  }

  public static void setUserSession(CalciteSchema schema, UserSession session) {
    if (schema == null) {
      return;
    }
    if (schema instanceof DynamicSchema) {
      ((DynamicSchema)schema).setSession(session);
    } else if (schema instanceof DynamicRootSchema) {
      ((DynamicRootSchema)schema).setSession(session);
    }
  }
}
