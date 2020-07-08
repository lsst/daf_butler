.. _daf_butler_formatters_assemblers:

#########################
Formatters and Assemblers
#########################

Formatters and assemblers provide the interface between Butler and the python types it is storing and retrieving.
A Formatter is responsible for serializing a Python type to an external storage system and reading that serialized form back into Python.
The serialization can be stored to a local file system or cloud storage or even a database.
It is possible for a formatter to be globally configured to use particular parameters on write.
On retrieval of datasets read parameters can be used that can, for example, return only a subset of the data.

Assemblers are used to disassemble and reassemble composite datasets and can also be used to process read parameters that adjust how the retrieved dataset might be modified on get.

Storage Classes
===============

A Storage Class is fundamental to informing Butler how to deal with specific Python types.
Each `~lsst.daf.butler.DatasetType` is associated with a `~lsst.daf.butler.StorageClass`.
This storage class declares the Python type, any components it may have (read-only or read-write), and an assembler that can be used to process read parameters.

Composites
^^^^^^^^^^

A composite storage class declares that the Python type consists of discrete components that can be accessed individually.
Each of these components must declare its storage class as well.

For example, if a ``pvi`` dataset type has been associated with an ``ExposureF`` storage class, the user can `~lsst.daf.butler.Butler.get()` the full ``pvi`` and access the components as they would normally for an `~lsst.afw.image.ExposureF`, or if the user solely want the metadata header from the exposure they can ask for ``pvi.metadata`` and just get that.
The implementation details of how that metadata is retrieved depend on the details of how the dataset was serialized within the datastore.

Composites must declare **all** the components for the Python type and Butler requires that if a composite is dissassembled into its components and then reassembled to form the composite again, this operation must be lossless.

Only composites have the potential to be disassembled into discrete file artifacts by datastore.
Disassembly itself is controlled by the datastore configuration.
In cases where the datastore uses a remote storage and only some components are required, then disassembly can significantly improve data access performance.

Read-only Components
^^^^^^^^^^^^^^^^^^^^

There are some situations where a Python type has some property that can usefully be retrieved that looks like a component of a composite but is not a component since it is not an integral part of the composite Python type.
This is particularly true for metadata such as bounding boxes or pixel counts which can efficiently be obtained from a large dataset without requiring that large dataset to be read into memory solely to be discarded once this information is extracted.

Read-only components can be defined for all storage classes without that storage class being declared to be a composite.
As for standard components, a read-only component declares the storage class (and therefore Python type) of that component.
If your Python type has useful components that can be accessed but which do not support full disassembly (because round-tripping disassembly is lossy), read-only components should be defined rather than full-fledged components.

Read Parameters
^^^^^^^^^^^^^^^

A storage class definition includes read parameters that can control how a particular storage class is modified by `~lsst.daf.butler.Butler.get()` before being returned to the caller.
These read parameters can be thought of as being understood by the Python type being returned and modifying it in a way that still returns that identical Python type.
The canonical example of this is subsetting where the caller passes in a bounding box that reduces the size of the image being returned.

If a parameter would change the Python type its functionality should be encapsulated in a read-only component.

.. note::

  Parameters that control how to serialize a dataset into an artifact within the datastore are not supported by user code doing a `~lsst.daf.butler.Butler.put()`.
  This is because a user storing a dataset does not know which formatter the datastore has been configured to use or even if a dataset will be persisted.
  For example, the caller has no real idea whether a particular compression option is supported or not because they have no visibility into whether the file written is in HDF5 or FITS or plain text.
  For this reason write parameters are part of formatter configuration.

Read Parameters and Read-Only Components
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Read parameters are usually applied during the retrieval of the associated Python type from the persisted artifact.
This requires that the parameters are understood by that Python type.
When read-only components involve read parameters there are now multiple ways in which the parameters can be applied.

Consider the case of a pixel counting read-only component for an image dataset.
A read parameter for subsetting the dataset should be applied to the image before the pixel counting is performed.
It does not make sense for subsetting to be applied to the integer pixel count.

For this reason read parameters for read-only components are processed prior to calculating the read-only component.

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
    assembler: lsst.something.MaskedImageAssembler
    parameters:
      - subset
    components:
      image: Image
      mask: Mask
    readComponents:
      npixels: NumPixels

In this simplified definition for a masked image, there are two components declared along with a read-only component that returns the number of pixels in the image.
The assembler should be able to disassemble the associated Python type into the ``image`` and ``mask`` components if the datastore requests disassembly.
The assembler would also be used to process the ``subset`` read parameter if the formatter used by the datastore has declared it does not support the parameter.

In some cases you may want to define specific storage classes that are specializations of a more generic definition.
You can do this using YAML anchors and references but the preferred approach is to use the ``inheritsFrom`` key in the storage class definition:

.. code-block:: yaml

   MaskedImageI:
     inheritsFrom: MaskedImage
     pytype: lsst.afw.image.MaskedImageI
     components:
       image: ImageI
       mask: MaskX

If this approach is used the `~lsst.daf.butler.StorageClass` Python class created by `~lsst.daf.butler.StorageClassFactory` will inherit from the specific parent class and not the generic `~lsst.daf.butler.StorageClass`.

Assemblers
==========

.. note::

  The base class is called CompositeAssembler because it was first developed for composites.
  Now might be a good time to rebrand it since non-composites need one if they use read parameters.

Every `~lsst.daf.butler.StorageClass` that defines read parameters or components (read/write or read) must also specify an `~lsst.daf.butler.CompositeAssembler` class.
This class should inherit from the `~lsst.daf.butler.CompositeAssembler` base class.

Composite Disassembly
^^^^^^^^^^^^^^^^^^^^^

A composite is declared by specifying components in the `~lsst.daf.butler.StorageClass` definition.
Assemblers must provide at minimum a `~lsst.daf.butler.CompositeAssembler.getComponent()` method to enable a specific component to be extracted from the composite Python type.
Datastores can be configured to prefer to write composite datasets out as the individual components and to reconstruct the composite on read.
This can lead to more efficient use of datastore bandwidth (especially an issue for an S3-like storage rather than a local file system) if a pipeline always takes as input a component and does not require the full dataset or if a user in the science platform wants to retrieve the metadata for many datasets.
To allow this the assembler subclass must provide `~lsst.daf.butler.CompositeAssembler.assemble()` and `~lsst.daf.butler.CompositeAssembler.disassemble()`.

Datastores can be configured to always disassemble composites or never disassemble them.
Additionally datastores can choose to only disassemble specific storage classes or dataset types.

.. warning::

  Composite disassembly implicitly assumes that an identical Python object can be created from the disassembled components.
  If this is not true, the components should be declared read-only and disassembly will never be attempted.

Read-only Components
^^^^^^^^^^^^^^^^^^^^

Just as for components of a composite, if a storage class defines read-only components, it must also specify an assembler to support the calculation of that derived component.
This should be implemented in the `~lsst.daf.butler.CompositeAssembler.getComponent()` method.

Additionally, if the storage class refers to a composite, the datastore can be configured to disassemble the dataset into discrete artifacts.
Since read-only components are derived and are not persisted themselves the datastore needs to be told which component should be used to calculate this derived quantity.
To enable this the assembler must implement `~lsst.daf.butler.CompositeAssembler.selectResponsibleComponent()`.
This method is given the name of the read-only component and a list of all available persisted components and must return one and only one relevant component.
The datastore will then make a component request to the formatter associated with that component.

.. note::

  All assemblers must support read/write components and read components in the `~lsst.daf.butler.CompositeAssembler.getComponent()` implementation method.
  As a corollary, all storage classes using components must define an assembler.

Read Parameters
^^^^^^^^^^^^^^^

Read parameters are used to adjust what is returned by the `~lsst.daf.butler.Butler.get()` call but there is a requirement that whatever those read parameters do to modify the `~lsst.daf.butler.Butler.get()` the Python type returned must match the type associated with the `~lsst.daf.butler.Butler.StorageClass` associated with the `~lsst.daf.butler.Butler.DatasetType`.
For example this means that a read parameter that subsets an image is valid because the type returned would still be an image.

If read parameters are defined then a `~lsst.daf.butler.CompositeAssembler.handleParameters()` method must be defined that understands how to apply these parameters to the Python object and should return a modified copy.
This method must be written even if a `~lsst.daf.butler.Formatter` is to be used.
There are two reasons for this, firstly, there is no guarantee that a particular formatter implementation will understand the parameter (and no requirement for that to be the case), and secondly there is no guarantee that a formatter will be involved in retrieval of the dataset.
In-memory datastores never involve a file artifact so whilst composite disassembly is never an issue, an assembler must at least provide the parameter handler to allow the user to configure such a datastore.

For read-only components parameters are handled by the composite component prior to deriving the read-only component.
The assembler `~lsst.daf.butler.CompositeAssembler.handleParameters()` method will only be called in this situation if no formatter is used (such as with an in-memory datastore).

Formatters
==========

Formatters are responsible for serializing a Python type to a storage system and for reconstructing the Python type from the serialized form.
A formatter has to implement at minimum a `~lsst.daf.butler.Formatter.read()` method and a `~lsst.daf.butler.Formatter.write()` method.
The ``write()`` method takes a Python object and serializes it somewhere and the ``read()`` method is optionally given a component name and returns the matching Python object.
Details of where the artifact may be located within the datastore are passed to the constructor as a `~lsst.daf.butler.FileDescriptor` instance.

.. warning::

  The formatter system has only been used to write datasets to files or to bytes that would be written to a file.
  The interface may evolve as other types of datastore become available and make use of the formatter system.


Configuring Formatters
^^^^^^^^^^^^^^^^^^^^^^

Formatter configuration matches on dataset type, storage class, or data ID as described in :ref:`daf_butler-config-lookups`.
