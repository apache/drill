<#-- Licensed to the Apache Software Foundation (ASF) under one or more contributor
  license agreements. See the NOTICE file distributed with this work for additional
  information regarding copyright ownership. The ASF licenses this file to
  You under the Apache License, Version 2.0 (the "License"); you may not use
  this file except in compliance with the License. You may obtain a copy of
  the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required
  by applicable law or agreed to in writing, software distributed under the
  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS
  OF ANY KIND, either express or implied. See the License for the specific
  language governing permissions and limitations under the License. -->

<#--
  Add implementations of additional parser statements here.
  Each implementation should return an object of SqlNode type.

  Example of SqlShowTables() implementation:
  SqlNode SqlShowTables()
  {
    ...local variables...
  }
  {
    <SHOW> <TABLES>
    ...
    {
      return SqlShowTables(...)
    }
  }
-->
/**
 * Parses statement
 *   SHOW TABLES [{FROM | IN} db_name] [LIKE 'pattern' | WHERE expr]
 */
SqlNode SqlShowTables() :
{
    SqlParserPos pos;
    SqlIdentifier db = null;
    SqlNode likePattern = null;
    SqlNode where = null;
}
{
    <SHOW> { pos = getPos(); }
    <TABLES>
    [
        (<FROM> | <IN>) { db = CompoundIdentifier(); }
    ]
    [
        <LIKE> { likePattern = StringLiteral(); }
        |
        <WHERE> { where = Expression(ExprContext.ACCEPT_SUBQUERY); }
    ]
    {
        return new SqlShowTables(pos, db, likePattern, where);
    }
}

/**
 * Parses statement
 * SHOW FILES [{FROM | IN} schema]
 */
SqlNode SqlShowFiles() :
{
    SqlParserPos pos = null;
    SqlIdentifier db = null;
}
{
    <SHOW> { pos = getPos(); }
    <FILES>
    [
        (<FROM> | <IN>) { db = CompoundIdentifier(); }
    ]
    {
        return new SqlShowFiles(pos, db);
    }
}


/**
 * Parses statement SHOW {DATABASES | SCHEMAS} [LIKE 'pattern' | WHERE expr]
 */
SqlNode SqlShowSchemas() :
{
    SqlParserPos pos;
    SqlNode likePattern = null;
    SqlNode where = null;
}
{
    <SHOW> { pos = getPos(); }
    (<DATABASES> | <SCHEMAS>)
    [
        <LIKE> { likePattern = StringLiteral(); }
        |
        <WHERE> { where = Expression(ExprContext.ACCEPT_SUBQUERY); }
    ]
    {
        return new SqlShowSchemas(pos, likePattern, where);
    }
}

/**
 * Parses statement
 *   { DESCRIBE | DESC } tblname [col_name | wildcard ]
 */
SqlNode SqlDescribeTable() :
{
    SqlParserPos pos;
    SqlIdentifier table;
    SqlIdentifier column = null;
    SqlNode columnPattern = null;
}
{
    (<DESCRIBE> | <DESC>) { pos = getPos(); }
    table = CompoundIdentifier()
    (
        column = CompoundIdentifier()
        |
        columnPattern = StringLiteral()
        |
        E()
    )
    {
        return new SqlDescribeTable(pos, table, column, columnPattern);
    }
}

SqlNode SqlUseSchema():
{
    SqlIdentifier schema;
    SqlParserPos pos;
}
{
    <USE> { pos = getPos(); }
    schema = CompoundIdentifier()
    {
        return new SqlUseSchema(pos, schema);
    }
}

/** Parses an optional field list and makes sure no field is a "*". */
SqlNodeList ParseOptionalFieldList(String relType) :
{
    SqlNodeList fieldList;
}
{
    fieldList = ParseRequiredFieldList(relType)
    {
        return fieldList;
    }
    |
    {
        return SqlNodeList.EMPTY;
    }
}

/** Parses a required field list and makes sure no field is a "*". */
SqlNodeList ParseRequiredFieldList(String relType) :
{
    SqlNodeList fieldList;
}
{
    <LPAREN>
    fieldList = SimpleIdentifierCommaList()
    <RPAREN>
    {
        for(SqlNode node : fieldList)
        {
            if (((SqlIdentifier)node).isStar())
                throw new ParseException(String.format("%s's field list has a '*', which is invalid.", relType));
        }
        return fieldList;
    }
}

/**
 * Parses a create view or replace existing view statement.
 *   CREATE [OR REPLACE] VIEW view_name [ (field1, field2 ...) ] AS select_statement
 */
SqlNode SqlCreateOrReplaceView() :
{
    SqlParserPos pos;
    boolean replaceView = false;
    SqlIdentifier viewName;
    SqlNode query;
    SqlNodeList fieldList;
}
{
    <CREATE> { pos = getPos(); }
    [ <OR> <REPLACE> { replaceView = true; } ]
    <VIEW>
    viewName = CompoundIdentifier()
    fieldList = ParseOptionalFieldList("View")
    <AS>
    query = OrderedQueryOrExpr(ExprContext.ACCEPT_QUERY)
    {
        return new SqlCreateView(pos, viewName, fieldList, query, replaceView);
    }
}

/**
 * Parses a drop view or drop view if exists statement.
 * DROP VIEW [IF EXISTS] view_name;
 */
SqlNode SqlDropView() :
{
    SqlParserPos pos;
    boolean viewExistenceCheck = false;
}
{
    <DROP> { pos = getPos(); }
    <VIEW>
    [ <IF> <EXISTS> { viewExistenceCheck = true; } ]
    {
        return new SqlDropView(pos, CompoundIdentifier(), viewExistenceCheck);
    }
}

/**
 * Parses a CTAS statement.
 * CREATE TABLE tblname [ (field1, field2, ...) ] AS select_statement.
 */
SqlNode SqlCreateTable() :
{
    SqlParserPos pos;
    SqlIdentifier tblName;
    SqlNodeList fieldList;
    SqlNodeList partitionFieldList;
    SqlNode query;
}
{
    {
        partitionFieldList = SqlNodeList.EMPTY;
    }
    <CREATE> { pos = getPos(); }
    <TABLE>
    tblName = CompoundIdentifier()
    fieldList = ParseOptionalFieldList("Table")
    (   <PARTITION> <BY>
        partitionFieldList = ParseRequiredFieldList("Partition")
    )?
    <AS>
    query = OrderedQueryOrExpr(ExprContext.ACCEPT_QUERY)
    {
        return new SqlCreateTable(pos, tblName, fieldList, partitionFieldList, query);
    }
}

/**
 * Parses a drop table or drop table if exists statement.
 * DROP TABLE [IF EXISTS] table_name;
 */
SqlNode SqlDropTable() :
{
    SqlParserPos pos;
    boolean tableExistenceCheck = false;
}
{
    <DROP> { pos = getPos(); }
    <TABLE>
    [ <IF> <EXISTS> { tableExistenceCheck = true; } ]
    {
        return new SqlDropTable(pos, CompoundIdentifier(), tableExistenceCheck);
    }
}

/**
 * Parse refresh table metadata statement.
 * REFRESH TABLE METADATA tblname
 */
SqlNode SqlRefreshMetadata() :
{
    SqlParserPos pos;
    SqlIdentifier tblName;
    SqlNodeList fieldList;
    SqlNode query;
}
{
    <REFRESH> { pos = getPos(); }
    <TABLE>
    <METADATA>
    tblName = CompoundIdentifier()
    {
        return new SqlRefreshMetadata(pos, tblName);
    }
}

/**
* Parses statement
*   DESCRIBE { SCHEMA | DATABASE } name
*/
SqlNode SqlDescribeSchema() :
{
   SqlParserPos pos;
   SqlIdentifier schema;
}
{
   <DESCRIBE> { pos = getPos(); }
   (<SCHEMA> | <DATABASE>) { schema = CompoundIdentifier(); }
   {
        return new SqlDescribeSchema(pos, schema);
   }
}