.. _lsst.daf.butler-dev_data_coordinate:

.. py:currentmodule:: lsst.daf.butler

Design notes for `DataCoordinate`
---------------------------------

Overview
^^^^^^^^

`DataCoordinate` is one of the most important classes in daf_butler, and one of
the trickiest to implement.
This is its fourth iteration, with each being
quite different from the other.
There are two big reasons for this:

- We want this object to behave much like a `dict`, because plain `dict` objects are frequently used as informal data IDs, but Python's `Mapping` ABC specifies some behavior that isn't quite compatible with how data IDs conceptually behave.
  In particular, its definition of how `__eq__` relates to `keys` isn't what we want.
  That leaves us stuck in tradeoff space between a (slight) Liskov substitutability violation, not actually making this a `Mapping`, and not making a data ID behave the way one would expect given what it represents.
  See the class docs for a more complete description of the problem and how the current (4th) `DataCoordinate` handles it (the 2nd version took the same approach).
  In essence, it makes `keys` less convenient in some contexts (in that it doesn't always contain all of the keys the object actually knows about) in order to make the Liskov substitution violation in `__eq__` even smaller (in that it affects only comparisons between `DataCoordinate` and other `Mapping` classes, not those between two `DataCoordinate` instances).

- `DataCoordinate` objects that represent the same fundamental identifier can have many different states, reflecting how much "identified information" the object carries relative to what is in the database.
  While a data ID can of course identify a lot of different kinds of information (especially if combined with other things, like a dataset type and/or collection), here we're concerned only with information that's so "close" to the identifiers that it really does make sense (we think) to pass it around with the data ID.

  That information includes:

    A) Dimension key-value pairs for implied dimensions (e.g. physical_filter in a data ID that includes, say, exposure).
       These are "close" to the data ID because whether they are required or implied depends on the full set of dimensions, and hence changes from data ID to data ID (in the example above, a physical_filter key-value pair _is_ required if the data ID does not have an exposure key-value pair).

    B) DimensionRecord objects for some or all elements.
       These include alternate keys (e.g. string names for detectors) and other metadata that we might want to include in filename templates, as well as the spatial and temporal information we need to query for other data IDs
       related to this one.

There's actually even an aspect of (A) that we're not (yet) dealing with - we may in the future want to allow alternate sets of identifiers for a particular set of dimensions (e.g. dayObs + seqNum in place of integer IDs for LSST exposures).
That means we could have a data ID that does have sufficient information to fully identify a set of dimensions, but it's not the canonical set of identifiers, and hence we can't tell whether that data ID is equivalent to some other one until we've done a database query to get to canonical form.
I'm hoping that when that day comes we'll not try to make those kinds of data IDs into DataCoordinates, but even an adjacent class that doesn't try to maintain DataCoordinate's invariants or support its full interface will add some complexity.

History
^^^^^^^

I don't actually recall much about the first version, which I think wasn't actually even called DataCoordinate, and was based on a very different earlier version of the dimensions subpackage.
I'm not going to bother refreshing my memory now, as most of that is now irrelevant, and I do remember its fatal flaw: it had too many states, especially with regard to what DimensionRecords (or earlier equivalent) it held.
That was really hard on code that needed to make a state transition (say, fetch some additional records from the database so it could fill in a filename template).
Should it fetch everything it needs, regardless of whether some of it was already present, or is the complexity of trying to compute a diff between what it has and what it needs worthwhile?  Should it fetch more than it needs, because one fetch for many records can be a lot more efficient than N separate fetches?
The second version is what we've had in the codebase since ~June 2019.
This split version the problem across two concrete classes, `DataCoordinate` and its subclass ``ExpandedDataCoordinate``.
`DataCoordinate` was truly minimal, containing key-value pairs for required dimensions only, and ``ExpandedDataCoordinate`` was truly maximal, containing key-value pairs for all dimensions and DimensionRecords for all elements.  Both were mappings that define `keys` to contain only required dimensions, with ``ExpandedDataCoordinate`` adding a `full` attribute that provided access to all dimensions as well as direct support for all dimensions in `__getitem__`.
The biggest problem with the second version was that it didn't quite have enough states: queries for related data IDs (Registry.queryDimensions, soon to be Registry.queryDataIds) naturally return key-value pairs for all dimensions (not just required dimensions) but do not naturally return DimensionRecords for all elements - and in fact those were quite expensive to fetch later, because despite the fact that we'd rewritten the DataCoordinate classes so we wouldn't have to, we still did the fetches one element and one data ID at a time.
So (prior to DM-24938) we were stuck in an unpleasant tradeoff space when querying for data IDs:

 - we could throw away a lot of the information we got for free and stuff it in a `DataCoordinate`,

 - or we could do a lot of expensive follow-up queries for `DimensionRecord`\ s we might not need in order to be able to use an ``ExpandedDataCoordinate`` and keep it that information we got for free.

That led to the third version, which right now looks like it will be squashed away in the history of ``DM-24938`` or its subtask-issues and never see the light of the master branch.
In that version, I made ``DataCoordinate`` a non-mapping ABC that implemented only `__getitem__`, and added intermediate ABCs ``MinimalDataCoordinate`` (mapping with required dimension keys), ``CompleteDataCoordinate`` (mapping with all-dimensions keys), and ``ExpandedDataCoordinate`` (subclass of ``CompleteDataCoordinate`` that adds ``DimensionRecord``\ s for all elements).
Concrete implementations of each of these were private and constructed only via static or class methods on the public ABCs, and it was all very clean in an OO-heavy, strongly typed sense.
It was also involved some multiple inheritance (including diamonds) down at the implementation level, but of the sort that still feels okay because the pure ABCs were are the right places in the hierarchy and super() did what you want and if it smelled a little like Java it smelled like _nice_ Java.
It started to feel less nice when I started adding custom containers for `DataCoordinate` objects on the same tickets, and immediately had to choose between:

 - having one suite of containers for all DataCoordinate types, with a lot of generics in the type annotations _and_ some runtime type information to semi-gracefully handle the fact that you could do more with containers of some DataCoordinate types than you could with others;

 - one suite of containers for each public DataCoordinate ABC, with messy inheritance relationships (MI that *didn't* smell so nice) and lots of boilerplate methods that often just changed a return type to some covariant type.

After some tinkering, I went uneasily with the former.
What really killed version three in the end was realizing that my clever "I'll make the base `DataCoordinate` not a `Mapping`"  and "Make Complete inherit from DataCoordinate but not from Minimal" ideas didn't _actually_ avoid the Liskov substitution in `__eq__` problem fully; it was just as bad as what I had before (in that comparisons between instances of the same type were consistent with `Mapping`, but those between different types still were not).
That meant that splitting the interface into four different ABCs was really only useful as a way to get MyPy to tell us when we didn't have the type we thought.
But that same strong typing was a lot of what made the containers a mess, so it wasn't really a net win.
Hence version four (what's below, unless this comment has gotten out of date).
I've kept what I liked about version three - putting the interface in an ABC and the implementations in private classes, and an intermediate "all dimension key-value pairs, but no records" state - but moved the state checking from the type system to runtime flags (``hasFull`` and ``hasRecords``).
It's ``hasFull`` instead of (say) `isComplete` because we can now keep the `full` attribute from version 2 and break a lot less code.
So, version four has single `DataCoordinate` ABC that can be in any of three states:

 - ``hasFull() is False``, ``hasRecords() is False`` - like ``MinimalDataCoordinate``, has only required dimension key-value pairs;

 - ``hasFull() is True``, ``hasRecords() is False`` - like ``CompleteDataCoordinate``, has all dimension key-value pairs but no records;

 - ``hasFull() is True``, ``hasRecords() is True`` - like ``ExpandedDataCoordinate``, has everything.

We happen to have two implementation classes - the first and second cases can use the same - but that's not really central to the design.
The only real messiness here is the fact that `DataCoordinate` defines a lot of operations that are only valid when the right status flags are set, and MyPy can't help us catch problems with that.
But it's still going to be much more familiar and understandable to most of our Python devs (who don't use type annotations) and no less safe in all of our non-daf_butler codebase (which doesn't use MyPy).

Minor points
^^^^^^^^^^^^

The advantages of having an ABC separate from the implementation classes are not as clear-cut as they were in version three.
Partly it's just good practice (it's the "D" in SOLID).
It also gives us the freedom to implement the three runtime states in one, two, or three (or more) classes for efficiency (or other) reasons.
What really sold me on keeping that split here was that it makes it easy to hide the constructors of the concrete classes from the users - in version two, we very prominently documented them as being for advanced/low-level use only, but they still ended up being used in contexts outside daf_butler where `DataCoordinate.standardize` would have been much safer.
And because you only get one constructor in Python, if you want to have a version that basically just takes the class's state (say, so you can use it to implement an efficient factory method), that's the only constructor you get.
I think staticmethod and classmethod constructors are often the best workaround for that (the alternative is having a ton of argments to `__init__`, which comes a different set of tradeoffs), but if your `__init__` isn't _also_ safe for general use or expose implementation details, it's a real pain to actually get users to not use it.
When the whole implementation class is clearly marked as private, that problem goes away.
