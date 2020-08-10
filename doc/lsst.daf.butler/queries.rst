.. py:currentmodule:: lsst.daf.butler

.. _daf_butler_queries:

.. py:currentmodule:: lsst.daf.butler

Querying datasets
=================

Datasets in a butler-managed data repository are identified by the combination of their *dataset type* and *data ID* within a *collection*.
The `Registry` class's query methods (`~Registry.queryDatasetTypes`, `~Registry.queryCollections`, `~Registry.queryDataIds`, and `~Registry.queryDatasets`) allow these to be specified either fully or partially in various ways.

.. _daf_butler_dataset_type_expressions:

DatasetType expressions
-----------------------

Arguments that specify one or more dataset types can generally take any of the following:

 - `DatasetType` instances;
 - `str` values (corresponding to `DatasetType.name`);
 - `re.Pattern` values (matched to `DatasetType.name` strings, via `~re.Pattern.fullmatch`);
 - iterables of any of the above;
 - the special value "``...``", which matches all dataset types.

Some of these are not allowed in certain contexts (as documented there).

.. _daf_butler_collection_expressions:

Collection expressions
----------------------

Arguments that specify one or more collections are similar to those for dataset types; they can take:

 - `str` values (the full collection name);
 - `re.Pattern` values (matched to the collection name, via `~re.Pattern.fullmatch`);
 - a `tuple` of (`str`, *dataset-type-restriction*) - see below;
 - iterables of any of the above;
 - the special value "``...``", which matches all collections;
 - a mapping from `str` to *dataset-type-restriction*.

A *dataset-type-restriction* is a :ref:`DatasetType expression <daf_butler_dataset_type_expressions>` that limits a search for datasets in the associated collection to just the specified dataset types.
Unlike most other DatasetType expressions, it may not contain regular expressions (but it may be "``...``", which is the implied value when no
restriction is given, as it means "no restriction").
In contexts where restrictions are meaningless (e.g. `~Registry.queryCollections` when the ``datasetType`` argument is `None`) they are allowed but ignored.

Collection expressions are processed by the `~registry.wildcards.CollectionQuery`, and `~registry.wildcards.DatasetTypeRestriction` classes.
User code will rarely need to interact with these directly, but they can be passed to `Registry` instead of the expression objects themselves, and hence may be useful as a way to transform an expression that may include single-pass iterators into an equivalent form that can be reused.

Ordered collection searches
^^^^^^^^^^^^^^^^^^^^^^^^^^^

An *ordered* collection expression is required in contexts where we want to search collections only until a dataset with a particular dataset type and data ID is found.
These include all direct `Butler` operations, the definitions of `~CollectionType.CHAINED` collections, `Registry.findDataset`, and the ``deduplicate=True`` mode of `Registry.queryDatasets`.
In these contexts, regular expressions and "``...``" are not allowed for collection names, because they make it impossible to unambiguously define the order in which to search.
Dataset type restrictions are allowed in these contexts, and those
may be (and usually are) "``...``".

Ordered collection searches are processed by the `~registry.wildcards.CollectionSearch` class.

.. _daf_butler_dimension_expressions:

Dimension expressions
---------------------

Constraints on the data IDs returned by a query can take two forms:

 - an explicit data ID value can be provided (as a `dict` or `DataCoordinate` instance) to directly constrain the dimensions in the data ID and indirectly constrain any related dimensions (see :ref:`Dimensions Overview <lsst.daf.butler-dimensions_overview>`);

 - a string expression resembling a SQL WHERE clause can be provided to constrain dimension values in a much more general way.

In most cases, the two can be provided together, requiring that returned data IDs match both constraints.
The rest of this section describes the latter in detail.

The language grammar is defined in the ``exprParser.parserYacc`` module, which is responsible for transforming a string with the user expression into a syntax tree with nodes represented by various classes defined in the ``exprParser.exprTree`` module.
Modules in the ``exprParser`` package are considered butler/registry implementation details and are not exposed at the butler package level.

The grammar is based on standard SQL; it is a subset of SQL expression language that can appear in WHERE clause of standard SELECT statement with some extensions, such as range support for the ``IN`` operator and time literals.

Expression structure
^^^^^^^^^^^^^^^^^^^^

The expression is passed as a string via the ``where`` arguments of `~Registry.queryDataIds` and `~Registry.queryDatasets`.
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

General note --- the parser itself does not evaluate any expressions even if
they consist of literals only, all evaluation happens in the SQL engine when
registry runs the resulting SQL query.

Following sections describe each of the parts in detail.

Literals
^^^^^^^^

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

Time literals
    Timestamps in a query are defined using special syntax which consists of
    a capital letter "T" followed by quoted string: ``T'time-string'``. Time
    string contains time information together with optional time format and
    time scale. For detailed description of supported time specification
    check section :ref:`time-literals-syntax`.

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
^^^^^^^^^^^

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
^^^^^^^^^^^^^^^^^^^^^^^^^^

Two unary operators ``+`` (plus) and ``-`` (minus) can be used in the
expressions in front of (numeric) literals, identifiers, or other
expressions which should evaluate to a numeric value.

Binary arithmetic operators
^^^^^^^^^^^^^^^^^^^^^^^^^^^

Language supports five arithmetic operators: ``+`` (add), ``-`` (subtract),
``*`` (multiply), ``/`` (divide), and ``%`` (modulo). Usual precedence rules
apply to these operators. Operands for them can be anything that evaluates to
a numeric value.

Comparison operators
^^^^^^^^^^^^^^^^^^^^

Language supports set of regular comparison operators: ``=``, ``!=``, ``<``,
``<=``, ``>``, ``>=``. This can be used on operands that evaluate to a numeric
values or timestamps, for (in)equality operators operands can also be boolean
expressions.

.. note :: The equality comparison operator is a single ``=`` like in SQL, not
    double ``==`` like in Python or C++.


IN operator
^^^^^^^^^^^

The ``IN`` operator (and ``NOT IN``) are an expanded version of a regular SQL
IN operator. Its general syntax looks like:

.. code-block:: sql

    <expression> IN ( <literal1>[, <literal2>, ... ])
    <expression> NOT IN ( <literal1>[, <literal2>, ... ])

where each item in the right hand side list is one of the supported literals.
Unlike regular SQL IN operator the list cannot contain expressions, only
literals. The extension to regular SQL IN is that literals can be range
literals as defined above. It can also be a mixture of integer literals and
range literals (language allows mixing of string literals and ranges but it
may not make sense when translated to SQL).

For an example of range usage, these two expressions are equivalent:

.. code-block:: sql

   visit IN (100, 110, 130..145:5)
   visit in (100, 110, 130, 135, 140, 145)

as are these:

.. code-block:: sql

   visit NOT IN (100, 110, 130..145:5)
   visit Not In (100, 110, 130, 135, 140, 145)

Boolean operators
^^^^^^^^^^^^^^^^^

``NOT`` is the standard unary boolean negation operator.

``AND`` and ``OR`` are binary logical and/or operators.

All boolean operators can work on expressions which return boolean values.


Grouping operator
^^^^^^^^^^^^^^^^^

Parentheses should be used to change evaluation order (precedence) of
sub-expressions in the full expression.


.. _time-literals-syntax:

Time literals
^^^^^^^^^^^^^

Timestamps in a query language are specified using syntax ``T'time-string'``.
The content of the ``time-string`` specifies a time point in one of the
supported time formats. For internal time representation Registry uses
`astropy.time.Time`_ class and parser converts time string into an instance
of that class. For string-based time formats such as ISO the conversion
of a time string to an object is done by the ``Time`` constructor. The syntax
of the string could be anything that is suported by ``astropy``, for details
see `astropy.time`_ reference. For numeric time formats such as MJD the parser
converts string to a floating point number and passes that number to ``Time``
constructor.

Parser guesses time format from the content of the time string:

- If time string is a floating point number then parser assumes that time
  is in "mjd" format.
- If string matches ISO format then parser assumes "iso" or "isot" format
  depending on presence of "T" separator in a string.
- If string starts with "+" sign followed by ISO string then parser assumes
  "fits" format.
- If string matches ``year:day:time`` format then "yday" is used.

The format can be specified explicitely by prefixing time string with a format
name and slash, e.g. ``T'mjd/58938.515'``. Any of the formats supported by
``astropy`` can be specified explicitely.

Time scale that parser passes to ``Time`` constructor depends on time format,
by default parser uses:

- "utc" scale for "iso", "isot", "fits", "yday", and "unix" formats,
- "tt" scale for "cxcsec" format,
- "tai" scale for anything else.

Default scale can be overriden by adding a suffix to time string consisting
of a slash and time scale name, e.g. ``T'58938.515/tai'``. Any combination of
explicit time format and time scale can be given at the same time, e.g.
``T'58938.515'``, ``T'mjd/58938.515'``, ``T'58938.515/tai'``, and
``T'mjd/58938.515/tai'`` all mean the same thing.

Note that `astropy.time.Time`_ class imposes few restrictions on the format
of the string that it accepts for iso/isot/fits/yday formats, in particular:

- time zone specification is not supported
- hour-only time is not supported, at least minutes have to be specified for
  time (but time can be omitted entirely)

.. _astropy.time: https://docs.astropy.org/en/stable/time/
.. _astropy.time.Time: https://docs.astropy.org/en/stable/api/astropy.time.Time.html


Examples
^^^^^^^^

Few examples of valid expressions using some of the constructs:

.. code-block:: sql

    visit > 100 AND visit < 200

    visit IN (100..200) AND tract = 500

    visit IN (100..200) AND visit NOT IN (159, 191) AND abstract_filter = 'i'

    (visit = 100 OR visit = 101) AND exposure % 2 = 1

    visit.datetime_begin > T'2020-03-30 12:20:33'

    exposure.datetime_begin > T'58938.515'

    visit.datetime_end < T'mjd/58938.515/tai'
