.. _lsst.daf.butler-concrete_storage_classes_dataframe:

DataFrame
---------

The ``DataFrame`` storage class corresponds to the `pandas.DataFrame` class in Python.
It includes special support for dealing with multi-level indexes (i.e. `pandas.MultiIndex`) in columns.

Components
^^^^^^^^^^

The ``DataFrame`` storage class has a single component, ``columns``, which contains a description of the columns as a `pandas.Index` (often `pandas.MultiIndex`) instance.

Parameters
^^^^^^^^^^

The ``DataFrame`` storage clss supports a single parameter for partial reads, with the key ``columns``.
For single-level columns, this should be a single column name (`str`) or a `list` of column names.
For multi-level index (`pandas.MultiIndex`) columns, this should be a dictionary whose keys are the names of the levels, and whose values are column names (`str`) or lists thereof.
The loaded columns are the product of the values for all levels.
Levels not included in the dict are included in their entirety.

For example, the ``deepCoadd_obj`` dataset is typically defined as a hierarchical table with levels ``dataset``, ``filter``, and ``column``, which take values such as ``("meas", "HSC-R", "base_SdssShape_xx")``.
Retrieving this dataset via:

.. code-block:: python

   butler.get(
       "deepCoadd_obj", ...,
       parameters={
           "columns": {"dataset": "meas",
                       "filter": ["HSC-R", "HSC-I"],
                       "column": ["base_SdssShape_xx", "base_SdssShape_yy"]}
       }
   )

is equivalent to (but potentially much more efficient than):

.. code-block:: python

   full = butler.get("deepCoadd_obj", ...)
   full.loc[:, ["meas", ["HSC-R", "HSC-I"],
                ["base_SdssShape_xx", "base_SdssShape_yy"]]]
