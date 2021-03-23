.. _daf_butler_configuring:

Configuring a Butler
====================

A Butler repository is configured using YAML files.
The main sections of the YAML file are handled separately by each sub configuration.
Each config specialization, registry, schema, storage class, composites, and dimensions knows the name of the key for its own section of the configuration and knows the names of files providing overrides and defaults for the configuration.
Additionally, if the sub configuration contains a ``cls`` key, that class is imported and an additional configuration file name can be provided by asking the class for its defaultConfigFile  property.
All the keys within a sub configuration are processed by the class constructed from ``cls``.
The primary source of default values comes from the ``configs`` resource (accessed using `pkg_resources` and package ``lsst.daf.butler``) –- this directory contains YAML files matching the names specified in the sub config classes and also can include names specified by the corresponding component class (for example `~lsst.daf.butler.datastores.fileDatastore.FileDatastore`  specifies that configuration should be found in ``datastores/fileDatastore.yaml``.
There are additional search paths that can be included when a config object is constructed:

1. Explicit list of directory paths to search passed into the constructor.
2. Paths defined using the environment path variable ``$DAF_BUTLER_CONFIG_PATH``.

To construct a Butler configuration object (`~lsst.daf.butler.ButlerConfig`) from a file the following happens:

* The supplied config is read in.
* If any leaf nodes in the configuration end in ``includeConfigs`` the values (either a scalar or list) will be treated as the names of other config files.
  These files will be located either as an absolute path or relative to the current working directory, or the directory in which the original configuration file was found.
  Shell variables will be expanded.
  The contents of these files will then be inserted into the configuration at the same hierarchy as the ``includeConfigs`` directive, with priority given to the values defined explicitly in the parent configuration (for lists of include files later files overwrite content from earlier ones).
* Each sub configuration class is constructed by supplying the relevant subset of the global config to the component Config constructor.
* A search path is constructed by concatenating the supplied search path, the environment variable path (``$DAF_BUTLER_CONFIG_PATH``), and the daf_butler config directory (within the ``lsst.daf.butler`` package resource).
* Defaults are first read from the config class default file name (e.g., ``registry.yaml`` for `~lsst.daf.butler.Registry`, and ``datastore.yaml`` for `~lsst.daf.butler.Datastore`) and merged in priority order given in the search path.
  Every file of that name found in the search path is read and combined with the others.
* Then any ``cls``-specific config files are read, overriding the current defaults.
  For example if the specified class for a datastore is `~lsst.daf.butler.datastores.fileDatastore.FileDatastore` a config file named ``datastores/fileDatastore.yaml`` will be located.
* Finally, any child configurations are read as specified in ``cls.containerKey``  (assumed to be a list of configurations compatible with the current config class).
  This is to allow a, for example, `~lsst.daf.butler.datastores.chainedDatastore.ChainedDatastore`  to be able to expand the child `~lsst.daf.butler.datastores.fileDatastore.FileDatastore` configurations using the same rules.
* Values specified in the ``butler.yaml`` always take priority since it is assumed that the values explicitly defined in the butler YAML file must be more important than values read from defaults.

The name of the specialist configuration file to search for can be found by looking at the ``defaultConfigFile`` class property for the particular class.

We also have a YAML parser extension ``!include`` that can be used to pull in other YAML files before the butler specific config parsing happens.
This is very useful to allow reuse of YAML snippets but be aware that the path specified is relative to the file that contains the directive.
In many cases ``includeConfigs`` is a more robust approach to file inclusion as it handles overrides in a more predictable manner.

There is a command available to allow you to see how all these overrides and includes behave.

.. prompt:: bash

   butler config-dump --subset .registry.db ./repo/butler.yaml

Note the leading "``.``" to indicate that you are using a "``.``" delimiter to specify the hierarchy within the configuration.

Overriding Root Paths
---------------------

In addition to the configuration options described above, there are some values that have a special meaning.
For `~lsst.daf.butler.registry.RegistryConfig` and `~lsst.daf.butler.DatastoreConfig` the ``root`` key, which can be used to specify paths, can include values using the special tag ``<butlerRoot>``.
At run time, this tag will be replaced by a value derived from the location of the main butler configuration file, or else from the value of the ``root`` key found at the top of the butler configuration.

Currently, if you create a butler configuration file that loads another butler configuration file, via ``includeConfigs``, then any ``<butlerRoot>`` tags will be replaced with the location of the new file, not the original.
It is therefore recommended that an explicit ``root`` be defined at the top level when defining butler overrides via a new top level butler configuration.
