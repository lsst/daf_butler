Butler v22.0 2021-04-01
=======================

New Features
------------

* A Butler instance can now be configured with dataId defaults such as an instrument or skymap. [DM-27153]
* Add ``butler prune-datasets`` command. [DM-26689]
* Add ``butler query-dimension-records`` command [DM-27344]
* Add ``--unlink`` option to ``butler prune-collection`` command. [DM-28857]
* Add progress reporting option for long-lived commands. [DM-28964]
* Add ``butler associate`` command to add existing datasets to a tagged collection. [DM-26688]
* Add officially-supported JSON serialization for core Butler classes. [DM-28314]
* Allow ``butler.get()`` to support dimension record values such as exposure observing day or detector name in the dataID. [DM-27152]
* Add "direct" ingest mode to allow a file to be ingested retaining the full path to the original file. [DM-27478]

Bug Fixes
---------

* Fix temporal queries and clarify ``Timespan`` behavior. [DM-27985]

Other Changes and Additions
---------------------------

* Make ``ButlerURI`` class immutable. [DM-29073]
* Add ``ButlerURI.findFileResources`` method to walk the directory tree and return matching files. [DM-29011]
* Improve infrastructure for handling test repositories. [DM-23862]

Butler Datastores
-----------------

New Features
~~~~~~~~~~~~

* Implement basic file caching for use with remote datastores. [DM-29383]
* Require that a DataId always be available to a ``Formatter``. This allows formatters to do a consistency check such as comparing the physical filter in a dataId with that read from a file. [DM-28583]
* Add special mode to datastore to instruct it to ignore registry on ``get``. This is useful for Execution Butlers where registry knows in advance about all datasets but datastore does not. [DM-28648]
* Add ``forget`` method to instruct datastore to remove all knowledge of a dataset without deleting the file artifact. [DM-29106]

Butler Registry
---------------

New Features
~~~~~~~~~~~~

* Avoid long-lived connections to database. [DM-26302]
* Add option to flatten when setting a collection chain. [DM-29203]
