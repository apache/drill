<#--

    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.

-->
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
        return new DrillSqlDescribeTable(pos, table, column, columnPattern);
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
    Pair<SqlNodeList, SqlNodeList> fieldList;
}
{
    <LPAREN>
    fieldList = ParenthesizedCompoundIdentifierList()
    <RPAREN>
    {
        for(SqlNode node : fieldList.left)
        {
            if (((SqlIdentifier) node).isStar())
                throw new ParseException(String.format("%s's field list has a '*', which is invalid.", relType));
        }
        return fieldList.left;
    }
}

/**
* Rarses CREATE [OR REPLACE] command for VIEW, TABLE or SCHEMA.
*/
SqlNode SqlCreateOrReplace() :
{
    SqlParserPos pos;
    String createType = "SIMPLE";
    boolean isTemporary = false;
}
{
    <CREATE> { pos = getPos(); }
    [ <OR> <REPLACE> { createType = "OR_REPLACE"; } ]
    [ <TEMPORARY> { isTemporary = true; } ]
    (
        <VIEW>
            {
                if (isTemporary) {
                    throw new ParseException("Create view statement does not allow <TEMPORARY> keyword.");
                }
                return SqlCreateView(pos, createType);
            }
    |
        <TABLE>
            {
                if (createType == "OR_REPLACE") {
                    throw new ParseException("Create table statement does not allow <OR><REPLACE>.");
                }
                return SqlCreateTable(pos, isTemporary);

            }
    |
        <SCHEMA>
             {
                 if (isTemporary) {
                     throw new ParseException("Create schema statement does not allow <TEMPORARY> keyword.");
                 }
                 return SqlCreateSchema(pos, createType);
             }
    )
}

/**
 * Parses a create view or replace existing view statement.
 * after CREATE OR REPLACE VIEW statement which is handled in the SqlCreateOrReplace method.
 *
 * CREATE { [OR REPLACE] VIEW | VIEW [IF NOT EXISTS] | VIEW } view_name [ (field1, field2 ...) ] AS select_statement
 */
SqlNode SqlCreateView(SqlParserPos pos, String createType) :
{
    SqlIdentifier viewName;
    SqlNode query;
    SqlNodeList fieldList;
}
{
    [
        <IF> <NOT> <EXISTS> {
            if (createType == "OR_REPLACE") {
                throw new ParseException("Create view statement cannot have both <OR REPLACE> and <IF NOT EXISTS> clause");
            }
            createType = "IF_NOT_EXISTS";
        }
    ]
    viewName = CompoundIdentifier()
    fieldList = ParseOptionalFieldList("View")
    <AS>
    query = OrderedQueryOrExpr(ExprContext.ACCEPT_QUERY)
    {
        return new SqlCreateView(pos, viewName, fieldList, query, SqlLiteral.createCharString(createType, getPos()));
    }
}

/**
 * Parses a CTAS or CTTAS statement after CREATE [TEMPORARY] TABLE statement
 * which is handled in the SqlCreateOrReplace method.
 *
 * CREATE [TEMPORARY] TABLE [IF NOT EXISTS] tblname [ (field1, field2, ...) ] AS select_statement.
 */
SqlNode SqlCreateTable(SqlParserPos pos, boolean isTemporary) :
{
    SqlIdentifier tblName;
    SqlNodeList fieldList;
    SqlNodeList partitionFieldList;
    SqlNode query;
    boolean tableNonExistenceCheck = false;
}
{
    {
        partitionFieldList = SqlNodeList.EMPTY;
    }
    ( <IF> <NOT> <EXISTS> { tableNonExistenceCheck = true; } )?
    tblName = CompoundIdentifier()
    fieldList = ParseOptionalFieldList("Table")
    (   <PARTITION> <BY>
        partitionFieldList = ParseRequiredFieldList("Partition")
    )?
    <AS>
    query = OrderedQueryOrExpr(ExprContext.ACCEPT_QUERY)
    {
        return new SqlCreateTable(pos, tblName, fieldList, partitionFieldList, query,
                                    SqlLiteral.createBoolean(isTemporary, getPos()),
                                    SqlLiteral.createBoolean(tableNonExistenceCheck, getPos()));
    }
}

/**
* Parses create table schema statement after CREATE OR REPLACE SCHEMA statement
* which is handled in the SqlCreateOrReplace method.
*
* CREATE [OR REPLACE] SCHEMA
* [
*   LOAD 'file:///path/to/raw_schema'
* |
*   (
*     col1 int,
*     col2 varchar(10) not null
*   )
* ]
* [FOR TABLE dfs.my_table]
* [PATH 'file:///path/to/schema']
* [PROPERTIES ('prop1'='val1', 'prop2'='val2')]
*/
SqlNode SqlCreateSchema(SqlParserPos pos, String createType) :
{
    SqlCharStringLiteral schema = null;
    SqlNode load = null;
    SqlIdentifier table = null;
    SqlNode path = null;
    SqlNodeList properties = null;
}
{
    {
            token_source.pushState();
            token_source.SwitchTo(SCH);
    }
    (
        <LOAD>
        {
            load = StringLiteral();
        }
    |
        <PAREN_STRING>
        {
            schema = SqlLiteral.createCharString(token.image, getPos());
        }
    )
    (
        <FOR> <TABLE> { table = CompoundIdentifier(); }
        |
        <PATH>
        {
            path = StringLiteral();
            if (createType == "OR_REPLACE") {
                throw new ParseException("<OR REPLACE> cannot be used with <PATH> property.");
            }
        }
    )
    [
        <PROPERTIES> <LPAREN>
        {
            properties = new SqlNodeList(getPos());
            addProperty(properties);
        }
        (
            <COMMA>
            { addProperty(properties); }
        )*
        <RPAREN>
    ]
    {
        return new SqlSchema.Create(pos, schema, load, table, path, properties,
            SqlLiteral.createCharString(createType, getPos()));
    }
}

/**
* Helper method to add string literals divided by equals into SqlNodeList.
*/
void addProperty(SqlNodeList properties) :
{}
{
    { properties.add(StringLiteral()); }
    <EQ>
    { properties.add(StringLiteral()); }
}

<SCH> SKIP :
{
    " "
|   "\t"
|   "\n"
|   "\r"
}

<SCH> TOKEN : {
    < LOAD: "LOAD" > { popState(); }
  | < NUM: <DIGIT> (" " | "\t" | "\n" | "\r")* >
    // once schema is found, swich back to initial lexical state
    // must be enclosed in the parentheses
    // inside may have left parenthesis only if number precededs (covers cases with varchar(10)),
    // if left parenthesis is present in column name, it must be escaped with backslash
  | < PAREN_STRING: <LPAREN> ((~[")"]) | (<NUM> ")") | ("\\)"))+ <RPAREN> > { popState(); }
}

/**
 * Parses DROP command for VIEW, TABLE and SCHEMA.
 */
SqlNode SqlDrop() :
{
    SqlParserPos pos;
}
{
    <DROP> { pos = getPos(); }
    (
        <VIEW>
        {
            return SqlDropView(pos);
        }
    |
        <TABLE>
        {
            return SqlDropTable(pos);
        }
    |
        <SCHEMA>
        {
            return SqlDropSchema(pos);
        }
    )
}

/**
 * Parses a drop view or drop view if exists statement
 * after DROP VIEW statement which is handled in SqlDrop method.
 *
 * DROP VIEW [IF EXISTS] view_name;
 */
SqlNode SqlDropView(SqlParserPos pos) :
{
    boolean viewExistenceCheck = false;
}
{
    [ <IF> <EXISTS> { viewExistenceCheck = true; } ]
    {
        return new SqlDropView(pos, CompoundIdentifier(), viewExistenceCheck);
    }
}

/**
 * Parses a drop table or drop table if exists statement
 * after DROP TABLE statement which is handled in SqlDrop method.
 *
 * DROP TABLE [IF EXISTS] table_name;
 */
SqlNode SqlDropTable(SqlParserPos pos) :
{
    boolean tableExistenceCheck = false;
}
{
    [ <IF> <EXISTS> { tableExistenceCheck = true; } ]
    {
        return new SqlDropTable(pos, CompoundIdentifier(), tableExistenceCheck);
    }
}

/**
* Parses drop schema or drop schema if exists statement
* after DROP SCHEMA statement which is handled in SqlDrop method.
*
* DROP SCHEMA [IF EXISTS]
* FOR TABLE dfs.my_table
*/
SqlNode SqlDropSchema(SqlParserPos pos) :
{
    SqlIdentifier table = null;
    boolean existenceCheck = false;
}
{
    [ <IF> <EXISTS> { existenceCheck = true; } ]
    <FOR> <TABLE> { table = CompoundIdentifier(); }
    {
        return new SqlSchema.Drop(pos, table, SqlLiteral.createBoolean(existenceCheck, getPos()));
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

/**
* Parse create UDF statement
* CREATE FUNCTION USING JAR 'jar_name'
*/
SqlNode SqlCreateFunction() :
{
   SqlParserPos pos;
   SqlNode jar;
}
{
   <CREATE> { pos = getPos(); }
   <FUNCTION>
   <USING>
   <JAR>
   jar = StringLiteral()
   {
       return new SqlCreateFunction(pos, jar);
   }
}

/**
* Parse drop UDF statement
* DROP FUNCTION USING JAR 'jar_name'
*/
SqlNode SqlDropFunction() :
{
   SqlParserPos pos;
   SqlNode jar;
}
{
   <DROP> { pos = getPos(); }
   <FUNCTION>
   <USING>
   <JAR>
   jar = StringLiteral()
   {
       return new SqlDropFunction(pos, jar);
   }
}

<#if !parser.includeCompoundIdentifier >
/**
* Parses a comma-separated list of simple identifiers.
*/
Pair<SqlNodeList, SqlNodeList> ParenthesizedCompoundIdentifierList() :
{
    List<SqlIdentifier> list = new ArrayList<SqlIdentifier>();
    SqlIdentifier id;
}
{
    id = SimpleIdentifier() {list.add(id);}
    (
   <COMMA> id = SimpleIdentifier() {list.add(id);}) *
    {
       return Pair.of(new SqlNodeList(list, getPos()), null);
    }
}
</#if>