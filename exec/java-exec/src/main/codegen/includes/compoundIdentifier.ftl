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
/**
 * Parses a Drill compound identifier.
 */
SqlIdentifier CompoundIdentifier() :
{
    DrillCompoundIdentifier.Builder builder = DrillCompoundIdentifier.newBuilder();
    String p;
    int index;
}
{
    p = Identifier()
    {
        builder.addString(p, getPos());
    }
    (
        (
        <DOT>
        (
            p = Identifier() {
                builder.addString(p, getPos());
            }
        |
            <STAR> {
                builder.addString("*", getPos());
            }
        )
        )
        |
        (
          <LBRACKET>
          index = UnsignedIntLiteral()
          <RBRACKET>
          {
              builder.addIndex(index, getPos());
          }
        )
    ) *
    {
      return builder.build();
    }
}

/**
 * Parses a compound identifier in the FROM clause.
 */
SqlIdentifier CompoundTableIdentifier() :
{
    final List<String> nameList = new ArrayList<String>();
    final List<SqlParserPos> posList = new ArrayList<SqlParserPos>();
}
{
    AddTableIdentifierSegment(nameList, posList)
    (
        LOOKAHEAD(2)
        <DOT>
          AddTableIdentifierSegment(nameList, posList)
    )*
    {
        SqlParserPos pos = SqlParserPos.sum(posList);
        return new SqlIdentifier(nameList, null, pos, posList);
    }
}

/**
 * Parses a comma-separated list of compound identifiers.
 */
void AddCompoundIdentifierTypeCommaList(List<SqlNode> list, List<SqlNode> extendList) :
{
}
{
    AddCompoundIdentifierType(list, extendList)
    (<COMMA> AddCompoundIdentifierType(list, extendList))*
}

/**
 * List of compound identifiers in parentheses. The position extends from the
 * open parenthesis to the close parenthesis.
 */
Pair<SqlNodeList, SqlNodeList> ParenthesizedCompoundIdentifierList() :
{
    final Span s;
    final List<SqlNode> list = new ArrayList<SqlNode>();
    final List<SqlNode> extendList = new ArrayList<SqlNode>();
}
{
    <LPAREN> { s = span(); }
    AddCompoundIdentifierTypeCommaList(list, extendList)
    <RPAREN> {
        return Pair.of(new SqlNodeList(list, s.end(this)), new SqlNodeList(extendList, s.end(this)));
    }
}
