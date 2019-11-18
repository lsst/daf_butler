.. _lsst.daf.butler-concrete_storage_classes_dataframe:

DataFrame
---------

The ``DataFrame`` storage class corresponds to the `pandas.DataFrame` class in Python.
It includes special support for dealing with hierarchical (i.e. `pandas.MultiIndex`) columns.

Components
^^^^^^^^^^

``DataFrame`` has a single component, ``columns``, which contains a description of the columns as a `pandas.Index` (often `pandas.MultiIndex`) instance.

Parameters
^^^^^^^^^^

``DataFrame`` supports a single parameter for partial reads, with the key ``columns``.
For non-hierachical columns, this should be a single column name (`str`) or a `list` of column names.
For hierarchical columns, this should be a dictionary whose keys are the names of the levels, and whose values are column names (`str`) or lists thereof.
The loaded columns are the product of the values for all levels.
Levels not included in the dict are included in their entirety.

For example, the ``deepCoadd_obj`` dataset is typically defined as a hierarchical table with levels ``dataset``, ``filter``, and ``column``, which take values such as ``("meas", "HSC-R", "base_SdssShape_xx")``.
Retrieving this dataset via::

    butler.get(
        "deepCoadd_obj", ...,
        parameters={
            "columns": {"dataset": "meas", "filter": ["HSC-R", "HSC-I"]}
        }
    )

is equivalent to (but potentially much more efficient than)::

  full = butler.get("deepCoadd_obj", ...)
  full.loc[:, ["meas", ["HSC-R", "HSC-I"]]]
