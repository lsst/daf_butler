.. _daf_butler_expr_parser:

Butler expression language
==========================

Butler registry supports a user-supplied expression for constraining input,
output, or intermediate datasets that can appear in the generated
QuantumGraph. This page describes the structure and syntax of the expression
language.

The language grammar is defined in ``exprParser.parserYacc`` module, which is
responsible for transforming string with user expression into a syntax tree
with nodes represented by various classes defined in ``exprParser.exprTree``
module. Modules in ``exprParser`` package are considered butler/registry
implementation details and are not exposed at the butler package level.

The gramma is based on standard SQL, it is a subset of SQL expression language
that can appear in WHERE clause of standard SELECT statement with some
extensions, e.g. range support for ``IN`` operator.

Expression structure
--------------------

The expression is passed as a string to a registry methods that build a
QuantumGraph typically from a command-line application such as ``pipetask``.
The string contains a single boolean expression which evaluates to true or
false (if it is a valid expression). Expression can contain a bunch of
standard logical operators, comparisons, literals, and identifiers which are
references to registry objects.

A few words in expression grammar are reserved: ``AND``, ``OR``, ``NOT`` and
``IN``. Reserved words are not case sensitive and can appear in either upper
or lower case, or a mixture of both.

Language operator precedence rules are the same as for the other languages
like C++ or Python. When in doubt use grouping operators (parentheses) for
sub-expressions.

General note - the parser itself does not evaluate any expressions even if
they consist of literals only, all evaluation happens in the SQL engine when
registry runs the resulting SQL query.

Following sections describe each of the parts in detail.

Literals
--------

The language supports these types of literals:

Strings
    This is just a sequence of characters enclosed in single quotation marks.
    The parser itself fully supports Unicode, but some tools such as database
    drivers may have limited support for it, depending on environment or
    encoding chosen.

Numbers
    Integer numbers are series of decimal numbers optionally preceded by
    minus sign. Parser does not support octal/hexadecimal numbers. Floating
    point numbers use standard notation with decimal point and/or exponent.
    For numbers parser passes a string representation of a number to
    downstream registry code to avoid possible rounding issues.

Range literals
    This sort of literal is allowed inside ``IN`` expressions only. It consists
    of two integer literals separated by double dots and optionally followed by
    a colon and one more integer literal. Two integers define start and stop
    values for the range; both are inclusive values. The optional third integer
    defines stride value, which defaults to 1; it cannot be negative. Ranges
    are equivalent to a sequence of integers (but not to intervals of floats).

Examples of range literals:

* ``1..5`` -- equivalent to ``1,2,3,4,5``
* ``1..10:3`` -- equivalent to ``1,4,7,10``
* ``-10..-1:2`` -- equivalent to ``-10,-8,-6,-4,-2``

Identifiers
-----------

Identifiers represent values external to a parser, such as values stored in a
database. The parser itself cannot define identifiers or their values; it is
the responsibility of translation layer (registry) to map identifiers into
something sensible. Like in most programming languages, an identifier starts
with a letter or underscore followed by zero or more letters, underscores, or
digits. Parser also supports dotted identifiers consisting of two simple
identifiers separated by a dot. Identifiers are case-sensitive on parser side
but individual database back-ends may have special rules about case
sensitivity.

In current implementation simple identifiers are used by registry to represent
dimensions, e.g. ``visit`` identifier is used to represent a value of
``visit`` dimension in registry database. Dotted identifiers are mapped to
tables and columns in registry database, e.g. ``detector.raft`` can be used
for accessing raft name (obviously dotted names need knowledge of database
schema and how SQL query is built).

Unary arithmetic operators
--------------------------

Two unary operators ``+`` (plus) and ``-`` (minus) can be used in the
expressions in front of (numeric) literals, identifiers, or other
expressions which should evaluate to a numeric value.

Binary arithmetic operators
---------------------------

Language supports five arithmetic operators - ``+`` (add), ``-`` (subtract),
``*`` (multiply), ``/`` (divide), and ``%`` (modulo). Usual precedence rules
apply to these operators. Operands for them can be anything that evaluates to
a numeric value.

Comparison operators
--------------------

Language supports set of regular comparison operators - ``=``, ``!=``, ``<``,
``<=``, ``>``, ``>=``. This can be used on operands that evaluate to a numeric
values, for (in)equality operators operands can also be boolean expressions.

.. note :: The equality comparison operator is a single ``=`` like in SQL, not
    double ``==`` like in Python or C++.


IN operator
-----------

The ``IN`` operator (and ``NOT IN``) are an expanded version of a regular SQL
IN operator. Its general syntax looks like::

    <expression> IN ( <literal1>[, <literal2>, ... ])
    <expression> NOT IN ( <literal1>[, <literal2>, ... ])

where each item in the right hand side list is one of the supported literals.
Unlike regular SQL IN operator the list cannot contain expressions, only
literals. The extension to regular SQL IN is that literals can be range
literals as defined above. It can also be a mixture of integer literals and
range literals (language allows mixing of string literals and ranges but it
may not make sense when translated to SQL).

For an example of range usage, these two expressions are equivalent::

    visit IN (100, 110, 130..145:5)
    visit in (100, 110, 130, 135, 140, 145)

as are these::

    visit NOT IN (100, 110, 130..145:5)
    visit Not In (100, 110, 130, 135, 140, 145)

Boolean operators
-----------------

``NOT`` is the standard unary boolean negation operator.

``AND`` and ``OR`` are binary logical and/or operators.

All boolean operators can work on expressions which return boolean values.


Grouping operator
-----------------

Parentheses should be used to change evaluation order (precedence) of
sub-expressions in the full expression.

Examples
--------

Few examples of valid expressions using some of the constructs::

    visit > 100 AND visit < 200

    visit IN (100..200) AND tract = 500

    visit IN (100..200) AND visit NOT IN (159, 191) AND abstract_filter = 'i'

    (visit = 100 OR visit = 101) AND exposure % 2 = 1
