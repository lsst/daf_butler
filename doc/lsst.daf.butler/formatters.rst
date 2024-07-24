.. _daf_butler_storageclass_formatters_delegates:

########################################################
Storage Classes, Storage Class Delegates, and Formatters
########################################################

.. py:currentmodule:: lsst.daf.butler

Formatters and storage class delegates provide the interface between Butler and the python types it is storing and retrieving.
A Formatter is responsible for serializing a Python type to an external storage system and reading that serialized form back into Python.
The serialization can be stored to a local file system or cloud storage or even a database.
It is possible for a formatter to be globally configured to use particular parameters on write.
On retrieval of datasets read parameters can be used that can, for example, return only a subset of the data.

Storage class delegates are used to disassemble and reassemble composite datasets and can also be used to process read parameters that adjust how the retrieved dataset might be modified on get.

Deciding which formatter or delegate to use is controlled by the storage class and corresponding dataset type.

.. note::

  When discussing configuration below, the default configuration values can be inspected at ``$DAF_BUTLER_DIR/python/lsst/daf/butler/configs`` (they can be accessed directly as Python package resources) and current values can be obtained by calling ``butler config-dump`` on a Butler repository.
  For example, this will list the formatter section of a butler configuration (assuming a single file-based datastore is in use):

  .. prompt:: bash

     butler config-dump --subset .datastore.formatters ./repo-dir

.. _daf_butler_storage_classes:

Storage Classes
===============

A Storage Class is fundamental to informing Butler how to deal with specific Python types.
Each `DatasetType` is associated with a `StorageClass` when it is defined (usually as part of a pipeline configuration).
This storage class declares the Python type, any components it may have (derived or read-write), and a delegate that can be used to process read parameters and do assembly.

Composites
^^^^^^^^^^

A composite storage class declares that the Python type consists of discrete components that can be accessed individually.
Each of these components must declare its storage class as well.

For example, if a ``pvi`` dataset type has been associated with an ``ExposureF`` composite storage class, the user can `Butler.get()` the full ``pvi`` and access the components as they would normally for an `~lsst.afw.image.ExposureF`, or if the user solely want the metadata header from the exposure they can ask for ``pvi.metadata`` and just get that.
The implementation details of how that metadata is retrieved depend on the details of how the dataset was serialized within the datastore.

Composites must declare **all** the components for the Python type and Butler requires that if a composite is dissassembled into its components and then reassembled to form the composite again, this operation must be lossless.

Only composites have the potential to be disassembled into discrete file artifacts by datastore.
Disassembly itself is controlled by the datastore configuration.
In cases where the datastore uses a remote storage and only some components are required, then disassembly can significantly improve data access performance.

Derived Components
^^^^^^^^^^^^^^^^^^

There are some situations where a Python type has some property that can usefully be retrieved that looks like a component of a composite but is not a component since it is not an integral part of the composite Python type.
This is particularly true for metadata such as bounding boxes or pixel counts which can efficiently be obtained from a large dataset without requiring that large dataset to be read into memory solely to be discarded once this information is extracted.

Derived components are read-only components that can be defined for all storage classes without that storage class being declared to be a composite.
As for standard components, a derived component declares the storage class (and therefore Python type) of that component.
If your Python type has useful components that can be accessed but which do not support full disassembly (because round-tripping disassembly is lossy), derived components should be defined rather than full-fledged components.

Read Parameters
^^^^^^^^^^^^^^^

A storage class definition includes read parameters that can control how a particular storage class is modified by `Butler.get()` before being returned to the caller.
These read parameters can be thought of as being understood by the Python type being returned and modifying it in a way that still returns that identical Python type.
The canonical example of this is subsetting where the caller passes in a bounding box that reduces the size of the image being returned.

If a parameter would change the Python type its functionality should be encapsulated in a derived component.

.. note::

  Parameters that control how to serialize a dataset into an artifact within the datastore are not supported by user code doing a `Butler.put()`.
  This is because a user storing a dataset does not know which formatter the datastore has been configured to use or even if a dataset will be persisted.
  For example, the caller has no real idea whether a particular compression option is supported or not because they have no visibility into whether the file written is in HDF5 or FITS or plain text.
  For this reason write parameters are part of formatter configuration.

Read Parameters and Derived Components
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Read parameters are usually applied during the retrieval of the associated Python type from the persisted artifact.
This requires that the parameters are understood by that Python type.
When derived components involve read parameters there are now multiple ways in which the parameters can be applied.

Consider the case of a pixel counting derived component for an image dataset.
A read parameter for subsetting the dataset should be applied to the image before the pixel counting is performed.
It does not make sense for subsetting to be applied to the integer pixel count.

For this reason read parameters for derived components are processed prior to calculating the derived component.

Type Converters
^^^^^^^^^^^^^^^

.. warning::
   The storage class conversion API is currently deemed to be experimental.
   It was developed to support dataset type migration.
   Do not add further converters without consultation.

It is sometimes convenient to be able to call `Butler.put` with a Python type that is not a match to the storage class defined for that dataset type in the registry.
Storage classes can be defined with converters that declare which Python types can be coerced into the required type, and what functions or method should be used to perform that conversion.
Butler can support this on `Butler.put` and `Butler.get`, the latter being required if the dataset type definition has been changed in registry after a dataset was stored.

Defining a Storage Class
^^^^^^^^^^^^^^^^^^^^^^^^

Storage classes are defined in the ``storageClasses`` section of the Butler configuration YAML file.
A very simple declaration of a storage class with an associated python type looks like:

.. code-block:: yaml

   NumPixels:
     pytype: int

This declares that the ``NumPixels`` storage class is defined as a Python `int`.
Nothing more is required for simple types.

A composite storage class refers to a Python type that can be disassembled into distinct components that can be retrieved independently:

.. code-block:: yaml

  MaskedImage:
    pytype: lsst.afw.image.MaskedImage
    delegate: lsst.something.MaskedImageDelegate
    parameters:
      - subset
    components:
      image: Image
      mask: Mask
    derivedComponents:
      npixels: NumPixels

In this simplified definition for a masked image, there are two components declared along with a derived component that returns the number of pixels in the image.
The delegate should be able to disassemble the associated Python type into the ``image`` and ``mask`` components if the datastore requests disassembly.
The delegate would also be used to process the ``subset`` read parameter if the formatter used by the datastore has declared it does not support the parameter.

In some cases you may want to define specific storage classes that are specializations of a more generic definition.
You can do this using YAML anchors and references but the preferred approach is to use the ``inheritsFrom`` key in the storage class definition:

.. code-block:: yaml

   GenericStorageClass:
      pytype: lsst.generic.GenericX
      components:
        image: ImageX
        metadata: Metadata
   GenericStorageClassI:
     inheritsFrom: GenericStorageClass
     pytype: lsst.generic.GenericI
     components:
       image: ImageI

If this approach is used the `StorageClass` Python class created by `StorageClassFactory` will inherit from the specific parent class and not the generic `StorageClass`.

Type converters are specified with a ``converters`` section.

.. code-block:: yaml

    StructuredDataDict:
      pytype: dict
      converters:
        lsst.daf.base.PropertySet: lsst.daf.base.PropertySet.toDict
    TaskMetadata:
      pytype: lsst.pipe.base.TaskMetadata
      converters:
        lsst.daf.base.PropertySet: lsst.pipe.base.TaskMetadata.from_metadata

In the first definition, the configuration says that if a ``PropertySet`` object is given then the unbound method ``lsst.daf.base.PropertSety.toDict`` can be called with the ``PropertySet`` as the only parameter and the returned value will be a `dict`.
In the second definition a ``PropertySet`` is again specified but this time the ``from_metadata`` class method will be called with the ``PropertySet`` as the first parameter and a ``TaskMetadata`` will be returned.

Storage Class Delegates
=======================

Every `StorageClass` that defines read parameters or components (read/write or derived) must also specify a storage class delegate class which should inherit from the `StorageClassDelegate` base class.

Composite Disassembly
^^^^^^^^^^^^^^^^^^^^^

A composite is declared by specifying components in the `StorageClass` definition.
Storage class delegate classes must provide at minimum a `StorageClassDelegate.getComponent()` method to enable a specific component to be extracted from the composite Python type.
Datastores can be configured to prefer to write composite datasets out as the individual components and to reconstruct the composite on read.
This can lead to more efficient use of datastore bandwidth (especially an issue for an S3-like storage rather than a local file system) if a pipeline always takes as input a component and does not require the full dataset or if a user in the science platform wants to retrieve the metadata for many datasets.
To allow this the delegate subclass must provide `StorageClassDelegate.assemble()` and `StorageClassDelegate.disassemble()`.

Datastores can be configured to always disassemble composites or never disassemble them.
Additionally datastores can choose to only disassemble specific storage classes or dataset types.

.. warning::

  Composite disassembly implicitly assumes that an identical Python object can be created from the disassembled components.
  If this is not true, the components should be declared derived (see next section) and disassembly will never be attempted.

Derived Components
^^^^^^^^^^^^^^^^^^

Just as for components of a composite, if a storage class defines derived components, it must also specify a delegate to support the calculation of that derived component.
This should be implemented in the `StorageClassDelegate.getComponent()` method.

Additionally, if the storage class refers to a composite, the datastore can be configured to disassemble the dataset into discrete artifacts.
Since derived components are computed and are not persisted themselves, the datastore needs to be told which component should be used to calculate this derived quantity.
To enable this the delegate must implement `StorageClassDelegate.selectResponsibleComponent()`.
This method is given the name of the derived component and a list of all available persisted components and must return one and only one relevant component.
The datastore will then make a component request to the `~lsst.daf.butler.Formatter` associated with that component.

.. note::

  All delegates must support read/write components and derived components in the `StorageClassDelegate.getComponent()` implementation method.
  As a corollary, all storage classes using components must specify a delegate.

.. note::

   A component returned by `~StorageClassDelegate.selectResponsibleComponent()` may require a custom formatter, to support the derived component, even if it otherwise would not.

Read Parameters
^^^^^^^^^^^^^^^

Read parameters are used to adjust what is returned by the `Butler.get()` call but there is a requirement that whatever those read parameters do to modify the `Butler.get()` the Python type returned must match the type associated with the `Butler.StorageClass` associated with the `Butler.DatasetType`.
For example this means that a read parameter that subsets an image is valid because the type returned would still be an image.

If read parameters are defined then a `StorageClassDelegate.handleParameters()` method must be defined that understands how to apply these parameters to the Python object and should return a modified copy.
This method must be written even if a `FormatterV2` is to be used.
There are two reasons for this; firstly, there is no guarantee that a particular formatter implementation will understand the parameter (and no requirement for that to be the case), and secondly there is no guarantee that a formatter will be involved in retrieval of the dataset.
In-memory datastores never involve a file artifact so whilst composite disassembly is never an issue, a delegate must at least provide the parameter handler to allow the user to configure such a datastore.

For derived components parameters are handled by the composite component prior to deriving the derived component.
The delegate `StorageClassDelegate.handleParameters()` method will only be called in this situation if no formatter is used (such as with an in-memory datastore).

Formatters
==========

Formatters are responsible for serializing a Python type to a storage system and for reconstructing the Python type from the serialized form.
A formatter author should define their formatter as a subclass of `FormatterV2`.

Reading a Dataset
^^^^^^^^^^^^^^^^^

A datastore knows which formatter was used to write or ingest a dataset.
There are three methods a formatter author can implement in order to read a Python type from a file:

``read_from_local_file``
    The ``read_from_local_file`` method is guaranteed to be passed a local file path.
    If the resource was initially remote it will be downloaded before calling the method and the file can be cached if the butler has been configured to do that.

``read_from_uri``
    The ``read_from_uri`` method is given a URI which might be local or remote and the method can access the resource directly.
    This can be especially helpful if the formatter can support partial reads of a remote resource if a component is requested or some parameters that subset the data.
    This file might be read from the local cache, if it is available, but will not trigger a download of the remote resource to the local cache.
    If the formatter is being called without a component or parameters such that the whole file would be read and if the dataset should be cached, this method will be called with a local file URI.

``read_from_stream``
    The ``read_from_stream`` method is given a file handle (usually a `lsst.resources.ResourceHandleProtocol`) which might be a local or remote resource.
    The resource might be read from local cache but the file will not be downloaded to the local cache prior to calling this method.
    If the file is being read without components or parameters and if it would be cached, this method will be bypassed if a file reader is available.

By default these methods are disabled by setting corresponding class properties named ``can_read_from_*`` to `False`.
To activate specific implementations a formatter author must set the corresponding properties to `True`.
Only one of these methods needs to be implemented by a formatter author, but if multiple options are available the priority order is specified in the `FormatterV2.read` documentation.
Any of these methods can be skipped by the formatter if it returns `NotImplemented`.

The read method has access to the storage class that was used to write the original dataset and the storage class that has been requested by the caller.
These are available in ``self.file_descriptor.storageClass`` (the one used for the write) and ``self.file_descriptor.readStorageClass``.
If a component is requested the read storage class will be that of the component.

Composite:

If "X" is the storage class of the dataset type associated with the registry, "Y" is the storage class of a component and "X'" and "Y'" are user overrides of those storage classes then:

========= ======   =======  ======
Component UserSC   WriteSC  ReadSC
========= ======   =======  ======
No            -        X        X
No            X'       X        X'
Yes           -        X        Y
Yes           Y'       X        Y'
========= ======   =======  ======

For a disassembled composite the file being opened by the formatter is a component and not directly the composite dataset.
In this situation the ``self.file_descriptor.component`` property will be set to indicate which component this file corresponds to and the ``self.dataset_ref`` property will refer to the composite dataset type.
As for the previous case, the write storage class will match the storage class used to write the file and the read storage class will be the storage class that has been requested and can either match the write storage class or be a user-provided override.
A component will be provided as a parameter solely in the cases where a derived component has been requested and in this scenario the read storage class will be the storage class of the derived component.
Any storage class override request for the composite will be applied by the storage class delegate if the composite has been disassembled and then reassembled.

Derived components will always set the read storage class to be that of the derived component, including any requested override.
If the original storage class for the derived component is required it can be obtained from the write storage class.

Writing a Dataset
^^^^^^^^^^^^^^^^^

When storing a dataset in a file datastore the datastore looks up the relevant formatter in the configuration based on the storage class or dataset name.
A formatter author can define one of the following methods to support writing:

``to_bytes``
    This method is given an in-memory dataset and returns the serialized bytes.

``write_local_file``
    This method is given an in-memory dataset and a local file name to write to.


When ingesting files from external sources formatters are associated with each incoming file but these formatters are only required to support reads.
They must though declare all the file extensions that they can support.
This allows the datastore to ensure that the image being ingested has not obviously been associated with a formatter that does not recognize it.

Some formatters can handle multiple Python types without requiring the datastore to force a conversion to a specific type before using the formatter.
A formatter that can support this should override the default `FormatterV2.can_accept` method such that it returns `True` for all supported Python types.

File Extensions
^^^^^^^^^^^^^^^

Each formatter that reads or writes a file must declare the file extensions that it supports.
For a formatter that supports a single extension this is most easily achieved by setting the class property `FormatterV2.default_extension` to that extension.
In some scenarios a formatter might support multiple formats that are controlled by write parameters.
In this case the formatter should assign a frozen set to the `FormatterV2.supported_extensions` class property.

It is then required that the subclass overrides the ``get_write_extension`` so  that it returns the extension that will be used by this formatter for writing the current dataset (for example by looking in the write parameters configuration).

Write Parameters
^^^^^^^^^^^^^^^^

Datastores can be configured to specify parameters that can control how a formatter serializes a Python object.
These configuration parameters are not available to `Butler` users as part of `Butler.put` since the user does not know how a datastore is configured or which formatter will be used for a particular `DatasetType`.

When datastore instantiates the `FormatterV2` the relevant write parameters are supplied.
These write parameters can be accessed when the data are written and they can control any aspect of the write.
The only caveat is that the `FormatterV2` read methods must be able to read the resulting file without having to know which write parameters were used to create it.
The read implementation methods can look at the file extension and file metadata but will not have the write parameters supplied to it by datastore.

Write Recipes
^^^^^^^^^^^^^

Sometimes you would like a formatter to be configured in the same way for all dataset types that use it but the configuration is very detailed.
An example of this is the configuration of data compression parameters for FITS files.
Rather than require that every formatter is explicitly configured with this detail, we have the concept of named write recipes.
Write recipes have their own configuration section and are associated with a specific formatter class and contain named collections of parameters.
The write parameters can then specify one of the named recipes by name.

If write recipes are used the formatter should implement a `FormatterV2.validate_write_recipes` method.
This method not only checks that the parameters are reasonable, it can also update the parameters with default values to make them self-consistent.

Configuring Formatters
^^^^^^^^^^^^^^^^^^^^^^

Formatter configuration matches on dataset type, storage class, or data ID as described in :ref:`daf_butler-config-lookups` and is present in the ``formatters`` section of the datastore YAML configuration.
The simplest configuration maps one of these keys to a fully-qualified python formatter class.
For example:

.. code-block:: yaml

   Defects: lsst.obs.base.formatters.fitsGeneric.FitsGenericFormatter
   Exposure: lsst.obs.base.formatters.fitsExposure.FitsExposureFormatter

Here we have two storage classes and they each point to a different formatter.

If a particular entry needs write parameters they can be defined by expanding the hierarchy:

.. code-block:: yaml

  Packages:
    formatter: lsst.obs.base.formatters.packages.PackagesFormatter
    parameters:
      format: yaml

Here the ``Packages`` storage class is associated with a formatter and the write parameters define one ``format`` option.

Sometimes it is required that every usage of a specific formatter should be configured in a uniform way.
This can be done using the magic ``default`` entry:

.. code-block:: yaml

  default:
    lsst.obs.base.formatters.fitsExposure.FitsExposureFormatter:
      # default is the default recipe regardless but this demonstrates
      # how to specify a default write parameter
      recipe: lossless

Here we are declaring that every write using the ``FitsExposureFormatter`` should by default be configured to use the ``lossless`` compression write recipe (the ``recipe`` parameter here is not special, but is understood by the formatter to mean a key into the write recipes configurations).
Parameters associated with a specific entry will be merged with the defaults.
This can allow lossless compression by default but allow specific dataset types to use lossy compression.

Write recipes also get their own magic key at the top level:

.. code-block:: yaml

  write_recipes:
    lsst.obs.base.formatters.fitsExposure.FitsExposureFormatter:
      recipe1:
        ...
      recipe2:
        ...

The write recipes are also grouped by formatter class and the ``...`` represent arbitrary yaml configuration associated with label ``recipe1`` and ``recipe2``.
