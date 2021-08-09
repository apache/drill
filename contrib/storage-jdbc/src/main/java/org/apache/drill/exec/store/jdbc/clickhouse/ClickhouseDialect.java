package org.apache.drill.exec.store.jdbc.clickhouse;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlBasicTypeNameSpec;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlUserDefinedTypeNameSpec;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.BasicSqlType;

/**
 * @author feiteng.wtf
 * @date 2021-08-08
 */
public class ClickhouseDialect extends SqlDialect {

  public static final SqlDialect DEFAULT =
    new ClickhouseDialect(EMPTY_CONTEXT
      .withDatabaseProduct(DatabaseProduct.UNKNOWN)
      .withIdentifierQuoteString("`"));


  public ClickhouseDialect(Context context) {
    super(context);
  }

  @Override
  public void unparseOffsetFetch(SqlWriter writer, SqlNode offset, SqlNode fetch) {
    unparseFetchUsingLimit(writer, offset, fetch);
  }

  @Override
  public SqlNode getCastSpec(RelDataType type) {
    switch (type.getSqlTypeName()) {
      case BOOLEAN:
        return new SqlDataTypeSpec(new SqlUserDefinedTypeNameSpec(type.isNullable()?
          "Nullable(UInt8)":"UInt8", SqlParserPos.ZERO), SqlParserPos.ZERO);
      case DECIMAL:
        return new SqlDataTypeSpec(
          new SqlBasicTypeNameSpec(type.getSqlTypeName(), type.getPrecision(),
            type.getScale(), type.getCharset() != null && supportsCharSet()
              ? type.getCharset().name() : null, SqlParserPos.ZERO), SqlParserPos.ZERO);
      default:
        if (type instanceof BasicSqlType) {
          return new SqlDataTypeSpec(new SqlUserDefinedTypeNameSpec(type.isNullable() ?
            String.format("Nullable(%s)",type.getSqlTypeName().name()):type.getSqlTypeName().name(), SqlParserPos.ZERO),
            SqlParserPos.ZERO);
        }
        throw new UnsupportedOperationException(String.format("cast to type " +
          "%s is not yet supported", type.getSqlTypeName().getName()));
    }
  }

}
