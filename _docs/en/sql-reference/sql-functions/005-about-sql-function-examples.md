---
title: "About SQL Function Examples"
slug: "About SQL Function Examples"
parent: "SQL Functions"
---
Historically it was necessary to use a FROM clause in Drill queries and many examples documented here still use a VALUES clause in the FROM clause to define rows of data in a derived table of statement level scope.  Since Drill 0.4.0, the FROM clause has been optional allowing you to test functions using briefer syntax, e.g.

    SELECT SQRT(2);

    |--------------------|
    | EXPR$0             |
    |--------------------|
    | 1.4142135623730951 |
    |--------------------|
