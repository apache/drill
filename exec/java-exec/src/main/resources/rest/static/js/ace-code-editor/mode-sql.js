/**
 * Drill SQL Definition (Forked from SqlServer definition)
 */

ace.define("ace/mode/sql_highlight_rules",["require","exports","module","ace/lib/oop","ace/mode/text_highlight_rules"], function(require, exports, module) {
"use strict";

var oop = require("../lib/oop");
var TextHighlightRules = require("./text_highlight_rules").TextHighlightRules;

var SqlHighlightRules = function() {

    //TODO: https://drill.apache.org/docs/reserved-keywords/
    //e.g. Cubing operators like ROLLUP are not listed
    //Covered: https://drill.apache.org/docs/supported-sql-commands/
    var keywords = (
        "select|insert|update|delete|from|where|and|or|group|by|order|limit|offset|having|as|case|" +
        "when|else|end|type|left|right|join|on|outer|desc|asc|union|create|table|key|if|lateral|apply|unnest|" +
        "not|default|null|inner|database|drop|" +
        "flatten|kvgen|columns|" +
        "set|reset|alter|session|system|" +
        "temporary|function|using|jar|between|distinct|" +
        "partition|view|schema|files|" +
        "explain|plan|with|without|implementation|" +
        "show|describe|use"
    );
    //Confirmed to be UnSupported as of Drill-1.12.0
    /* cross|natural|primary|foreign|references|grant */

    var builtinConstants = (
        "true|false"
    );

    //Drill-specific
    var builtinFunctions = (
        //Math and Trignometric
        "abs|cbrt|ceil|ceiling|degrees|e|exp|floor|log|log|log10|lshift|mod|negative|pi|pow|radians|rand|round|round|rshift|sign|sqrt|trunc|trunc|" +
        "sin|cos|tan|asin|acos|atan|sinh|cosh|tanh|" +
        //datatype conversion
        "cast|convert_to|convert_from|string_binary|binary_string|" +
        //time-conversion
        "to_char|to_date|to_number|to_timestamp|to_timestamp|" +
        "age|extract|current_date|current_time|current_timestamp|date_add|date_part|date_sub|localtime|localtimestamp|now|timeofday|unix_timestamp|" +
        //string manipulation
        "byte_substr|char_length|concat|ilike|initcap|length|lower|lpad|ltrim|position|regexp_replace|rpad|rtrim|strpos|substr|trim|upper|" +
        //statistical
        "avg|count|max|min|sum|stddev|stddev_pop|stddev_samp|variance|var_pop|var_samp|" +
        //null-handling
        "coalesce|nullif"
    );

    //Drill-specific
    var dataTypes = (
        "BIGINT|BINARY|BOOLEAN|CHAR|CHARACTER|DATE|DEC|DECIMAL|DOUBLE|FIXED16CHAR|FIXEDBINARY|FLOAT|INT|" +
        "INTEGER|INTERVAL|INTERVALDAY|INTERVALYEAR|NUMERIC|NULL|SMALLINT|TIME|TIMESTAMP|VARBINARY|" +
        "VAR16CHAR|VARCHAR");
    //[Cannot supported due to space]
    //DOUBLE PRECISION|CHARACTER VARYING;

    var keywordMapper = this.createKeywordMapper({
        "support.function": builtinFunctions,
        "keyword": keywords,
        "constant.language": builtinConstants,
        "storage.type": dataTypes
    }, "identifier", true);

    this.$rules = {
        "start" : [ {
            token : "comment",
            regex : "--.*$"
        },  {
            token : "comment",
            start : "/\\*",
            end : "\\*/"
        }, {
            token : "string",           // " string
            regex : '".*?"'
        }, {
            token : "string",           // ' string
            regex : "'.*?'"
        }, {
            token : "string",           // ` string (apache drill)
            regex : "`.*?`"
        }, {
            token : "constant.numeric", // float
            regex : "[+-]?\\d+(?:(?:\\.\\d*)?(?:[eE][+-]?\\d+)?)?\\b"
        }, {
            token : keywordMapper,
            regex : "[a-zA-Z_$][a-zA-Z0-9_$]*\\b"
        }, {
            token : "keyword.operator",
            regex : "\\+|\\-|\\/|\\/\\/|%|<@>|@>|<@|&|\\^|~|<|>|<=|=>|==|!=|<>|="
        }, {
            token : "paren.lparen",
            regex : "[\\(]"
        }, {
            token : "paren.rparen",
            regex : "[\\)]"
        }, {
            token : "text",
            regex : "\\s+"
        } ]
    };
    this.normalizeRules();
};

oop.inherits(SqlHighlightRules, TextHighlightRules);

exports.SqlHighlightRules = SqlHighlightRules;
});

ace.define("ace/mode/sql",["require","exports","module","ace/lib/oop","ace/mode/text","ace/mode/sql_highlight_rules"], function(require, exports, module) {
"use strict";

var oop = require("../lib/oop");
var TextMode = require("./text").Mode;
var SqlHighlightRules = require("./sql_highlight_rules").SqlHighlightRules;

var Mode = function() {
    this.HighlightRules = SqlHighlightRules;
    this.$behaviour = this.$defaultBehaviour;
};
oop.inherits(Mode, TextMode);

(function() {

    this.lineCommentStart = "--";

    this.$id = "ace/mode/sql";
}).call(Mode.prototype);

exports.Mode = Mode;

});
