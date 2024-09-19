.. py:currentmodule:: lsst.daf.butler

.. _daf_butler_queries:

.. py:currentmodule:: lsst.daf.butler

Querying datasets
=================

Methods for querying information about the datasets in a Butler repository can be found in three places:

- The main `Butler` object.  Key methods include `Butler.query_datasets`, `Butler.query_data_ids`, and `Butler.query_dimension_records`.
- The `ButlerCollections` object, accessed via `Butler.collections`.
- The `Registry` object, accessed via `Butler.registry`.  This interface is being phased out, so prefer to use the other methods when possible. `Registry.queryDatasetTypes` is one key method not yet available elsewhere.

Datasets in a butler-managed data repository are identified by the combination of their *dataset type* and *data ID* within a *collection*.
Query methods allow these to be specified either fully or partially in various ways.

.. note::
    Queries cache information about `DatasetType` definitions and "governor" metadata values associated with datasets.
    Concurrent writes by other butler clients may not be reflected in these caches, if they happened since this `Butler` was initialized.
    Users can call `Registry.refresh` before querying to update the caches.

.. _daf_butler_dataset_type_expressions:

DatasetType expressions
-----------------------

Arguments that specify dataset types can generally take either of:

 - `DatasetType` instances;
 - `str` values (corresponding to `DatasetType.name`);

Some methods (like `Registry.queryDatasetTypes`) also accept:

 - `str` values using glob wildcard syntax (like ``deepCoadd*`` to search for dataset types starting with the string "deepCoadd".)
 - iterables of any of the above;
 - the special value "``...``", which matches all dataset types.

`Registry.queryDatasetTypes` can be used to resolve patterns before calling other methods.

For most query methods, passing a dataset type or name that is not registered with the repository will result in `MissingDatasetTypeError` being raised.

.. _daf_butler_collection_expressions:

Collection expressions
----------------------

Arguments that specify one or more collections are similar to those for dataset types; they can take:

 - `str` values (the full collection name);
 - `str` values using glob wildcard syntax (like ``u/someone/*`` to find all collections starting with "u/someone/".)
 - iterables of any of the above, empty collection cannot match anything, methods always return an empty result set in this case;

Collection expressions are processed by the `~registry.wildcards.CollectionWildcard` class.
User code will rarely need to interact with these directly, but they can be passed to `Registry` instead of the expression objects themselves, and hence may be useful as a way to transform an expression that may include single-pass iterators into an equivalent form that can be reused.

.. _daf_butler_ordered_collection_searches:

Ordered collection searches
^^^^^^^^^^^^^^^^^^^^^^^^^^^

An *ordered* collection expression is required in contexts where we want to search collections only until a dataset with a particular dataset type and data ID is found.
These include:

- The definitions of `~CollectionType.CHAINED` collections
- `Butler.get`
- `Butler.find_dataset`
- The ``find_first=True`` (default) mode of `Butler.query_datasets`

In these contexts, regular expressions and "``...``" are not allowed for collection names, because they make it impossible to unambiguously define the order in which to search.

The `CollectionWildcard.require_ordered` method can be used to verify that a collection expression resolves to an ordered sequence.

.. _daf_butler_dimension_expressions:

Dimension expressions
---------------------

Constraints on the data IDs returned by a query can take two forms:

 - an explicit data ID value can be provided (as a `dict` or `DataCoordinate` instance) to directly constrain the dimensions in the data ID and indirectly constrain any related dimensions (see :ref:`Dimensions Overview <lsst.daf.butler-dimensions_overview>`);

 - a string expression resembling a SQL WHERE clause can be provided to constrain dimension values in a much more general way.

In most cases, the two can be provided together, requiring that returned data IDs match both constraints.
The rest of this section describes the latter in detail.

The grammar is based on standard SQL; it is a subset of SQL expression language that can appear in WHERE clause of standard SELECT statement with some extensions, such as range support for the ``IN`` operator and time literals.

Expression structure
^^^^^^^^^^^^^^^^^^^^

The expression is passed as a string via the ``where`` arguments of `~Butler.query_datasets`, `~Butler.query_data_ids`, and `~Butler.query_dimension_records`.
The string contains a single boolean expression which evaluates to true or
false (if it is a valid expression). Expression can contain a bunch of
standard logical operators, comparisons, literals, and identifiers which are
references to registry objects.

A few words in expression grammar are reserved: ``AND``, ``OR``, ``NOT``,
``IN``, and ``OVERLAPS``. Reserved words are not case sensitive and can appear
in either upper or lower case, or a mixture of both.

Language operator precedence rules are the same as for the other languages
like C++ or Python. When in doubt use grouping operators (parentheses) for
sub-expressions.

Following sections describe each of the parts in detail.

Literals
^^^^^^^^

The language supports these types of literals:

Strings
    This is just a sequence of characters enclosed in single quotation marks.

    Example: ``'some string'``

Numbers
    Integer numbers are series of decimal numbers optionally preceded by
    minus sign. Parser does not support octal/hexadecimal numbers. Floating
    point numbers use standard notation with decimal point and/or exponent.

    Examples:

    - ``1234``
    - ``-1``
    - ``1.2``
    - ``1.2e-5``

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

.. _daf_butler_dimension_expressions_identifiers:

Identifiers
^^^^^^^^^^^

Identifiers represent the names of dimensions and metadata values associated with them.

For example, ``visit`` identifier is used to represent a value of
``visit`` dimension in registry database. Dotted identifiers are mapped to
tables and columns in registry database, e.g. ``detector.raft`` can be used
for accessing raft name (obviously dotted names need knowledge of database
schema and how SQL query is built). A simple identifier with a name
``ingest_date`` is used to reference dataset ingest time, which can be used to
filter query results based on that property of datasets.

Registry methods accepting user expressions also accept a ``bind`` parameter, which is a mapping from identifier name to its corresponding value.
Identifiers appearing in user expressions will be replaced with the corresponding value from this mapping.
Using the ``bind`` parameter is encouraged when possible to simplify rendering of the query strings.
A partial example of comparing two approaches, without and with ``bind``:

.. code-block:: Python

    dataset_type = "calexp"

    instrument_name = "LSSTCam"
    visit_id = 12345

    # Direct rendering of query not using bind
    result = butler.query_datasets(
        dataset_type,
        where=f"instrument = '{instrument_name}' AND visit = {visit_id}",
    )

    # Same functionality using bind parameter
    result = butler.query_datasets(
        dataset_type,
        where="instrument = instrument_name AND visit = visit_id",
        bind={"instrument_name": instrument_name, "visit_id": visit_id},
    )

Types of values provided in a ``bind`` mapping must correspond to the expected type of the expression, which is usually a scalar type, one of ``int``, ``float``, ``str``, etc.
There is one context where a bound value can specify a list, tuple or set of values: an identifier appearing in the right-hand side of :ref:`expressions-in-operator`.
Note that parentheses after ``IN`` are still required when identifier is bound to a list or a tuple.
An example of this feature:

.. code-block:: Python

    instrument_name = "LSST"
    visit_ids = (12345, 12346, 12350)
    result = butler.query_datasets(
        dataset_type,
        where="instrument = instrument_name AND visit IN (visit_ids)",
        bind={"instrument_name": instrument_name, "visit_ids": visit_ids},
    )



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
values or timestamps.

.. note :: The equality comparison operator is a single ``=`` like in SQL, not
    double ``==`` like in Python or C++.

.. _expressions-in-operator:

IN operator
^^^^^^^^^^^

The ``IN`` operator (and ``NOT IN``) are an expanded version of a regular SQL
IN operator. Its general syntax looks like:

.. code-block:: sql

    <expression> IN ( <item1>[, <item2>, ... ])
    <expression> NOT IN ( <item1>[, <item2>, ... ])

where each item in the right hand side list is one of the supported literals
or identifiers. Unlike regular SQL IN operator the list cannot contain
expressions, only literals or identifiers. The extension to regular SQL IN is
that literals can be range literals as defined above.

Regular use of ``IN`` operator is for checking whether an integer number is in
set of numbers. For that case the list on right side can be a mixture of
integer literals, identifiers that represent integers, and range literals.

For an example of this type of usage, these two expressions are equivalent:

.. code-block:: sql

   visit IN (100, 110, 130..145:5)
   visit in (100, 110, 130, 135, 140, 145)

as are these:

.. code-block:: sql

   visit NOT IN (100, 110, 130..145:5)
   visit Not In (100, 110, 130, 135, 140, 145)

Another usage of ``IN`` operator is for checking whether a timestamp or a time
range is contained wholly in other time range. Time range in this case can be
specified as a tuple of two time literals or identifers each representing a
timestamp, or as a single identifier representing a time range. In case a
single identifier appears on the right side of ``IN`` it has to be enclosed
in parentheses.

Here are few examples for checking containment in a time range:

.. code-block:: sql

    -- using literals for both timestamp and time range
    T'2020-01-01' IN (T'2019-01-01', T'2020-01-01')
    (T'2020-01-01', T'2020-02-01') NOT IN (T'2019-01-01', T'2020-01-01')

    -- using identifiers for each timestamp in a time range
    T'2020-01-01' IN (interval.begin, interval.end)
    T'2020-01-01' NOT IN (interval_id)

    -- identifier on left side can represent either a timestamp or time range
    timestamp_id IN (interval.begin, interval.end)
    range_id NOT IN (interval_id)

OVERLAPS operator
^^^^^^^^^^^^^^^^^

The ``OVERLAPS`` operator checks for overlapping time ranges or regions, its
arguments have to have consistent types. Like with ``IN`` operator time ranges
can be represented with a tuple of two timestamps (literals or identifiers) or
with a single identifier.


Examples of the syntax for time ranges:

.. code-block:: sql

    (T'2020-01-01', T'2022-01-01') OVERLAPS (T'2019-01-01', T'2021-01-01')
    (interval.begin, interval.end) OVERLAPS interval_2
    interval_1 OVERLAPS interval_2

You can check for overlap of a region with a point using the ``POINT(ra, dec)`` syntax,
where ``ra`` and ``dec`` are specified as an ICRS sky position in degrees.

.. code-block:: sql

    visit.region OVERLAPS POINT(53.6, -32.7)

You can check overlaps with arbitrary sky regions by binding values (see
:ref:`daf_butler_dimension_expressions_identifiers`).  Bound region values may
be specified as the following object types:

-  ``lsst.sphgeom.Region``
-  ``lsst.sphgeom.LonLat``
-  ``astropy.coordinates.SkyCoord``

.. code-block:: sql

    visit.region OVERLAPS my_region

Boolean operators
^^^^^^^^^^^^^^^^^

``NOT`` is the standard unary boolean negation operator.

``AND`` and ``OR`` are binary logical and/or operators.

All boolean operators can work on expressions which return boolean values.


Grouping operator
^^^^^^^^^^^^^^^^^

Parentheses should be used to change evaluation order (precedence) of
sub-expressions in the full expression.


Function call
^^^^^^^^^^^^^

Function call syntax is similar to other languages, expression for call
consists of an identifier followed by zero or more comma-separated arguments
enclosed in parentheses (e.g. ``func(1, 2, 3)``). An argument to a function
can be any expression.

Presently there only one construct that uses this syntax, ``POINT(ra, dec)``
is function which declares (or returns) sky coordinates similarly to ADQL
syntax. Name of the ``POINT`` function is not case-sensitive.


.. _time-literals-syntax:

Time literals
^^^^^^^^^^^^^

Timestamps in a query language are specified using syntax ``T'time-string'``.
The content of the ``time-string`` specifies a time point in one of the
supported time formats. For internal time representation Registry uses
`astropy.time.Time`_ class and parser converts time string into an instance
of that class. For string-based time formats such as ISO the conversion
of a time string to an object is done by the ``Time`` constructor. The syntax
of the string could be anything that is supported by ``astropy``, for details
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

The format can be specified explicitly by prefixing time string with a format
name and slash, e.g. ``T'mjd/58938.515'``. Any of the formats supported by
``astropy`` can be specified explicitly.

Time scale that parser passes to ``Time`` constructor depends on time format,
by default parser uses:

- "utc" scale for "iso", "isot", "fits", "yday", and "unix" formats,
- "tt" scale for "cxcsec" format,
- "tai" scale for anything else.

Default scale can be overridden by adding a suffix to time string consisting
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

    visit IN (100..200) AND visit NOT IN (159, 191) AND band = 'i'

    (visit = 100 OR visit = 101) AND exposure % 2 = 1

    visit.timespan.begin > T'2020-03-30 12:20:33'

    exposure.timespan.begin > T'58938.515'

    visit.timespan.end < T'mjd/58938.515/tai'

    ingest_date < T'2020-11-06 21:10:00'


.. _daf_butler_query_ordering:

Query result ordering
---------------------

Butler query methods (`Butler.query_datasets`, `Butler.query_data_ids`, and `Butler.query_dimension_records`) support ordering and limiting the number of the returned records.
These methods have ``order_by`` and ``limit`` parameters controlling this behavior.

The ``order_by`` parameter accepts a list of strings specifying columns/fields used for ordering. Each string can have one of the supported formats:

- A dimension name, corresponding to the value of the dimension primary key, e.g. ``"visit"``
- A dimension name and a field name separated bey a dot. Field name can refer to any of the dimension's metadata or key, e.g. ``"visit.name"``, ``"detector.raft"``. Special field names ``"timespan.begin"`` and ``"timespan.end"`` can be used for temporal dimensions (visit and exposure).
- A field name without dimension name, in that case field is searched in all dimensions used by the query, and it has to be unique. E.g. ``"cell_x"`` means the same as ``"patch.cell_x"``.
- To reverse ordering for the field it is prefixed with a minus sign, e.g. ``"-visit.timespan.begin"``.

The ``limit`` parameter accepts an integer specifying the maximum number of results to return.

Example of use of these two parameters:

.. code-block:: Python

    # Print ten latest visit records in reverse time order
    for record in butler.query_dimension_records("visit", order_by=["-timespan.begin"], limit=10):
        print(record)

.. _daf_butler_query_error_handling:

Error handling with Registry methods
------------------------------------

`Registry` methods typically raise exceptions when they detect problems with input parameters.
Documentation for these methods describes a set of exception classes and conditions in which exceptions are generated.
In most cases, these exceptions belong to one of the special exception classes defined in `lsst.daf.butler.registry` module, e.g. `~lsst.daf.butler.registry.DataIdError`, which have `~lsst.daf.butler.registry.RegistryError` as a common base class.
These exception classes are not exposed by the `lsst.daf.butler` module interface; to use these classes they need to be imported explicitly, e.g.:

.. code-block:: Python

    from lsst.daf.butler.registry import DataIdError, UserExpressionError

While class documentation should list most commonly produced exceptions, there may be other exceptions raised by its methods.
Code that needs to handle all types of exceptions generated by `Registry` methods should be prepared to handle other types of exceptions as well.

A few of the `Registry` query methods (`~Registry.queryDataIds`, `~Registry.queryDatasets`, and `~Registry.queryDimensionRecords`) return result objects.
These objects are iterables of the corresponding record types and typically they represent a non-empty result set.
In some cases these methods can return empty results without generating an exception, for example due to a combination of constraints excluding all existing records.
Result classes implement ``explain_no_results()`` method which can be used to try to identify the reason for an empty result.
It returns a list of strings, with each string a human-readable message describing the reason for an empty result.
This method does not always work reliably and can return an empty list even when result is empty.
In particular it cannot analyze user expression and identify which part of that expression is responsible for an empty result.
