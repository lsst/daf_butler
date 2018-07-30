# This file is part of daf_butler.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (http://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

"""Configuration control."""

import collections
import copy
import pprint
import os
import yaml
import sys
from yaml.representer import Representer

import lsst.utils

from .utils import doImport

yaml.add_representer(collections.defaultdict, Representer.represent_dict)

__all__ = ("Config", "ConfigSubset")

# PATH-like environment variable to use for defaults.
CONFIG_PATH = "DAF_BUTLER_CONFIG_PATH"

# UserDict and yaml have defined metaclasses and Python 3 does not allow
# multiple inheritance of classes with distinct metaclasses. We therefore have
# to create a new baseclass that Config can inherit from. This is because the
# metaclass syntax differs between versions


class _ConfigMeta(type(collections.UserDict), type(yaml.YAMLObject)):
    pass


class _ConfigBase(collections.UserDict, yaml.YAMLObject, metaclass=_ConfigMeta):
    pass


class Loader(yaml.CLoader):
    """YAML Loader that supports file include directives

    Uses ``!include`` directive in a YAML file to point to another
    YAML file to be included. The path in the include directive is relative
    to the file containing that directive.

        storageClasses: !include storageClasses.yaml

    Examples
    --------
    >>> with open('document.yaml', 'r') as f:
           data = yaml.load(f, Loader=Loader)

    Notes
    -----
    See https://davidchall.github.io/yaml-includes.html
    """

    def __init__(self, stream):
        super().__init__(stream)
        self._root = os.path.split(stream.name)[0]
        Loader.add_constructor('!include', Loader.include)

    def include(self, node):
        if isinstance(node, yaml.ScalarNode):
            return self.extractFile(self.construct_scalar(node))

        elif isinstance(node, yaml.SequenceNode):
            result = []
            for filename in self.construct_sequence(node):
                result += self.extractFile(filename)
            return result

        elif isinstance(node, yaml.MappingNode):
            result = {}
            for k, v in self.construct_mapping(node).iteritems():
                result[k] = self.extractFile(v)
            return result

        else:
            print("Error:: unrecognised node type in !include statement", file=sys.stderr)
            raise yaml.constructor.ConstructorError

    def extractFile(self, filename):
        filepath = os.path.join(self._root, filename)
        with open(filepath, 'r') as f:
            return yaml.load(f, Loader)


class Config(_ConfigBase):
    """Implements a datatype that is used by `Butler` for configuration
    parameters.

    It is essentially a `dict` with key/value pairs, including nested dicts
    (as values). In fact, it can be initialized with a `dict`. The only caveat
    is that keys may **not** contain dots (``.``). This is explained next:

    Config extends the `dict` api so that hierarchical values may be accessed
    with dot-delimited notation. That is, ``foo.getValue('a.b.c')`` is the
    same as ``foo['a']['b']['c']`` is the same as ``foo['a.b.c']``, and either
    of these syntaxes may be used.

    Storage formats supported:

    - yaml: read and write is supported.


    Parameters
    ----------
    other : `str` or `Config` or `dict`
        Other source of configuration, can be:

        - (`str`) Treated as a path to a config file on disk. Must end with
          '.yaml'.
        - (`Config`) Copies the other Config's values into this one.
        - (`dict`) Copies the values from the dict into this Config.

        If `None` is provided an empty `Config` will be created.
    """

    def __init__(self, other=None):

        collections.UserDict.__init__(self)

        if other is None:
            return

        if isinstance(other, collections.Mapping):
            self.update(other)
        elif isinstance(other, Config):
            self.data = copy.deepcopy(other.data)
        elif isinstance(other, str):
            # if other is a string, assume it is a file path.
            self.__initFromFile(other)
        else:
            # if the config specified by other could not be recognized raise a runtime error.
            raise RuntimeError("A Config could not be loaded from other:%s" % other)

    def ppprint(self):
        """helper function for debugging, prints a config out in a readable
        way in the debugger.

        use: pdb> print(myConfigObject.ppprint())

        Returns
        -------
        s : `str`
            A prettyprint formatted string representing the config
        """
        return pprint.pformat(self.data, indent=2, width=1)

    def __repr__(self):
        return self.data.__repr__()

    def __initFromFile(self, path):
        """Load a file from path.

        Parameters
        ----------
        path : `str`
            To a persisted config file.
        """
        if path.endswith('yaml'):
            self.__initFromYamlFile(path)
        else:
            raise RuntimeError("Unhandled config file type:%s" % path)

    def __initFromYamlFile(self, path):
        """Opens a file at a given path and attempts to load it in from yaml.

        Parameters
        ----------
        path : `str`
            To a persisted config file in YAML format.
        """
        with open(path, 'r') as f:
            self.__initFromYaml(f)

    def __initFromYaml(self, stream):
        """Loads a YAML config from any readable stream that contains one.

        Parameters
        ----------
        stream
            To a persisted config file in YAML format.

        Raises
        ------
        yaml.YAMLError
            If there is an error loading the file.
        """
        content = yaml.load(stream, Loader=Loader)
        if content is None:
            content = {}
        self.data = content
        return self

    def __getitem__(self, name):
        data = self.data
        for key in name.split('.'):
            if data is None:
                return None
            if key in data:
                data = data[key]
            else:
                try:
                    i = int(key)
                    data = data[i]
                except ValueError:
                    return None
        if isinstance(data, collections.Mapping):
            data = Config(data)
        return data

    def __setitem__(self, name, value):
        keys = name.split('.')
        last = keys.pop()
        if isinstance(value, collections.Mapping):
            d = {}
            cur = d
            for key in keys:
                cur[key] = {}
                cur = cur[key]
            cur[last] = value
            self.update(d)
        data = self.data
        for key in keys:
            # data could be a list
            if isinstance(data, collections.Sequence):
                data = data[int(key)]
            else:
                data = data.setdefault(key, {})
        try:
            data[last] = value
        except TypeError:
            data[int(last)] = value

    def __contains__(self, key):
        d = self.data
        keys = key.split('.')
        last = keys.pop()
        for k in keys:
            if isinstance(d, collections.Sequence):
                # Check sequence first because for lists
                # __contains__ checks whether value is found in list
                # not whether the index exists in list. When we traverse
                # the hierarchy we are interested in the index.
                try:
                    d = d[int(k)]
                except IndexError:
                    return False
            elif k in d:
                d = d[k]
            else:
                return False
        return last in d

    def update(self, other):
        """Like dict.update, but will add or modify keys in nested dicts,
        instead of overwriting the nested dict entirely.

        For example, for the given code:
        foo = {'a': {'b': 1}}
        foo.update({'a': {'c': 2}})

        Parameters
        ----------
        other : `dict` or `Config`
            Source of configuration:

            - If foo is a dict, then after the update foo == {'a': {'c': 2}}
            - But if foo is a Config, then after the update
              foo == {'a': {'b': 1, 'c': 2}}
        """
        def doUpdate(d, u):
            for k, v in u.items():
                if isinstance(d, collections.Mapping):
                    if isinstance(v, collections.Mapping):
                        r = doUpdate(d.get(k, {}), v)
                        d[k] = r
                    else:
                        d[k] = v
                else:
                    if isinstance(d, collections.Sequence):
                        try:
                            intk = int(k)
                        except ValueError:
                            # This will overwrite the entire sequence
                            d = {k: v}
                        else:
                            r = doUpdate(d[intk], v)
                            d[intk] = r
                    else:
                        d = {k: v}
            return d
        doUpdate(self.data, other)

    def merge(self, other):
        """Like Config.update, but will add keys & values from other that
        DO NOT EXIST in self.

        Keys and values that already exist in self will NOT be overwritten.

        Parameters
        ----------
        other : `dict` or `Config`
            Source of configuration:
        """
        otherCopy = copy.deepcopy(other)
        otherCopy.update(self)
        self.data = otherCopy.data

    def names(self, topLevelOnly=False):
        """Get the dot-delimited name of all the keys in the hierarchy.

        Notes
        -----
        This is different than the built-in method `dict.keys`, which will
        return only the first level keys.
        """
        if topLevelOnly:
            return list(self.keys())

        def getKeys(d, keys, base):
            for key in d:
                val = d[key]
                levelKey = base + '.' + key if base is not None else key
                keys.append(levelKey)
                if isinstance(val, collections.Mapping):
                    getKeys(val, keys, levelKey)
        keys = []
        getKeys(self.data, keys, None)
        return keys

    def asArray(self, name):
        """Get a value as an array. May contain one or more elements.

        Parameters
        ----------
        name : `str`
            Key
        """
        val = self.get(name)
        if isinstance(val, str):
            val = [val]
        elif not isinstance(val, collections.Container):
            val = [val]
        return val

    def __lt__(self, other):
        if isinstance(other, Config):
            other = other.data
        return self.data < other

    def __le__(self, other):
        if isinstance(other, Config):
            other = other.data
        return self.data <= other

    def __eq__(self, other):
        if isinstance(other, Config):
            other = other.data
        return self.data == other

    def __ne__(self, other):
        if isinstance(other, Config):
            other = other.data
        return self.data != other

    def __gt__(self, other):
        if isinstance(other, Config):
            other = other.data
        return self.data > other

    def __ge__(self, other):
        if isinstance(other, Config):
            other = other.data
        return self.data >= other

    #######
    # i/o #

    def dump(self, output):
        """Writes the config to a yaml stream.

        Parameters
        ----------
        output
            The YAML stream to use for output.
        """
        # First a set of known keys is handled and written to the stream in a
        # specific order for readability.
        # After the expected/ordered keys are weritten to the stream the
        # remainder of the keys are written to the stream.
        data = copy.copy(self.data)
        keys = ['defects', 'needCalibRegistry', 'levels', 'defaultLevel', 'defaultSubLevels', 'camera',
                'exposures', 'calibrations', 'datasets']
        for key in keys:
            try:
                yaml.safe_dump({key: data.pop(key)}, output, default_flow_style=False)
                output.write('\n')
            except KeyError:
                pass
        if data:
            yaml.safe_dump(data, output, default_flow_style=False)

    def dumpToFile(self, path):
        """Writes the config to a file.

        Parameters
        ----------
        path : `str`
            Path to the file to use for output.
        """
        with open(path, 'w') as f:
            self.dump(f)

    @staticmethod
    def overrideConfigParameters(configType, config, full, toupdate=None, tocopy=None):
            """Generic helper function for overriding specific config parameters

            Allows for named parameters to be set to new values in bulk, and
            for other values to be set by copying from a reference config.

            Parameters
            ----------
            configType : `ConfigSubset`
                Config type to use to extract relevant items from ``config``.
            config : `Config`
                A `Config` to update. Only the subset understood by
                the supplied `ConfigSubset` will be modified. Default values
                will not be inserted and the content will not be validated
                since mandatory keys are allowed to be missing until
                populated later by merging.
            full : `Config`
                A complete config with all defaults expanded that can be
                converted to a ``configType``. Read-only and will not be
                modified by this method. Values are read from here if
                ``tocopy`` is defined.

                Repository-specific options that should not be obtained
                from defaults when Butler instances are constructed
                should be copied from `full` to `Config`.
            toupdate : `dict`, optional
                A `dict` defining the keys to update and the new value to use.
                The keys and values can be any supported by `Config`
                assignment.
            tocopy : `tuple`, optional
                `tuple` of keys whose values should be copied from ``full``
                into ``config``.

            Raises
            ------
            ValueError
                Neither ``toupdate`` not ``tocopy`` were defined.
            """
            if toupdate is None and tocopy is None:
                raise ValueError("One of toupdate or tocopy parameters must be set.")

            # Extract the part of the config we wish to update
            localConfig = configType(config, mergeDefaults=False, validate=False)

            if toupdate:
                for key, value in toupdate.items():
                    localConfig[key] = value

            if tocopy:
                localFullConfig = configType(full, mergeDefaults=False)
                for key in tocopy:
                    localConfig[key] = localFullConfig[key]

            # Reattach to parent
            if configType.component in config:
                config[configType.component] = localConfig
            else:
                config.update(localConfig)


class ConfigSubset(Config):
    """Config representing a subset of a more general configuration.

    Subclasses define their own component and when given a configuration
    that includes that component, the resulting configuration only includes
    the subset.  For example, your config might contain ``schema`` if it's
    part of a global config and that subset will be stored. If ``schema``
    can not be found it is assumed that the entire contents of the
    configuration should be used.

    Default values are read from the environment or supplied search paths
    using the default configuration file name specified in the subclass.
    This allows a configuration class to be instantiated without any
    additional arguments.

    Additional validation can be specified to check for keys that are mandatory
    in the configuration.

    Parameters
    ----------
    other : `Config` or `str` or `dict`
        Argument specifying the configuration information as understood
        by `Config`
    validate : `bool`, optional
        If `True` required keys will be checked to ensure configuration
        consistency.
    mergeDefaults : `bool`, optional
        If `True` defaults will be read and the supplied config will
        be combined with the defaults, with the supplied valiues taking
        precedence.
    searchPaths : `list` or `tuple`, optional
        Explicit additional paths to search for defaults. They should
        be supplied in priority order. These paths have higher priority
        than those read from the environment in
        `ConfigSubset.defaultSearchPaths()`.
    """

    component = None
    """Component to use from supplied config. Can be None. If specified the
    key is not required. Can be a full dot-separated path to a component.
    """

    requiredKeys = ()
    """Keys that are required to be specified in the configuration.
    """

    defaultConfigFile = None
    """Name of the file containing defaults for this config class.
    """

    def __init__(self, other=None, validate=True, mergeDefaults=True, searchPaths=None):

        # Create a blank object to receive the defaults
        # Once we have the defaults we then update with the external values
        super().__init__()

        # Create a standard Config rather than subset
        externalConfig = Config(other)

        # Select the part we need from it
        # To simplify the use of !include we also check for the existence of
        # component.component (since the included files can themselves
        # include the component name)
        if self.component is not None:
            doubled = "{0}.{0}".format(self.component)
            # Must check for double depth first
            if doubled in externalConfig:
                externalConfig = externalConfig[doubled]
            elif self.component in externalConfig:
                externalConfig.data = externalConfig.data[self.component]

        # Default files read to create this configuration
        self.filesRead = []

        # Sometimes we do not want to merge with defaults.
        if mergeDefaults:

            # Supplied search paths have highest priority
            fullSearchPath = []
            if searchPaths:
                fullSearchPath.extend(searchPaths)

            # Read default paths from enviroment
            fullSearchPath.extend(self.defaultSearchPaths())

            # There are two places to find defaults for this particular config
            # - The "defaultConfigFile" defined in the subclass
            # - The class specified in the "cls" element in the config.
            #   Read cls after merging in case it changes.
            if self.defaultConfigFile is not None:
                self._updateWithConfigsFromPath(fullSearchPath, self.defaultConfigFile)

            # Can have a class specification in the external config (priority)
            # or from the defaults.
            pytype = None
            if "cls" in externalConfig:
                pytype = externalConfig["cls"]
            elif "cls" in self:
                pytype = self["cls"]

            if pytype is not None:
                try:
                    cls = doImport(pytype)
                except ImportError:
                    raise RuntimeError("Failed to import cls '{}' for config {}".format(pytype,
                                                                                        type(self)))
                defaultsFile = cls.defaultConfigFile
                if defaultsFile is not None:
                    self._updateWithConfigsFromPath(fullSearchPath, defaultsFile)

        # Now update this object with the external values so that the external
        # values always override the defaults
        self.update(externalConfig)

        if validate:
            self.validate()

    @classmethod
    def defaultSearchPaths(cls):
        """Read the environment to determine search paths to use for global
        defaults.

        Global defaults, at lowest priority, are found in the ``config``
        directory of the butler source tree. Additional defaults can be
        defined using the environment variable ``$DAF_BUTLER_CONFIG_PATHS``
        which is a PATH-like variable where paths at the front of the list
        have priority over those later.

        Returns
        -------
        paths : `list`
            Returns a list of paths to search. The returned order is in
            priority with the highest priority paths first. The butler config
            directory will always be at the end of the list.
        """
        # We can pick up defaults from multiple search paths
        # We fill defaults by using the butler config path and then
        # the config path environment variable in reverse order.
        defaultsPaths = []

        if CONFIG_PATH in os.environ:
            externalPaths = os.environ[CONFIG_PATH].split(os.pathsep)
            defaultsPaths.extend(externalPaths)

        # Find the butler configs
        defaultsPaths.append(os.path.join(lsst.utils.getPackageDir("daf_butler"), "config"))

        return defaultsPaths

    def _updateWithConfigsFromPath(self, searchPaths, configFile):
        """Search the supplied paths, merging the configuration values

        The values read will override values currently stored in the object.
        Every file found in the path will be read, such that the earlier
        path entries have higher priority.

        Parameters
        ----------
        searchPaths : `list`
            Paths to search for the supplied configFile. This path
            is the priority order, such that files read from the
            first path entry will be selected over those read from
            a later path.
        configFile : `str`
            File to locate in path. If absolute path it will be read
            directly and the search path will not be used.
        """
        if os.path.isabs(configFile):
            if os.path.exists(configFile):
                self.filesRead.append(configFile)
                self._updateWithOtherConfigFile(configFile)
        else:
            # Reverse order so that high priority entries
            # update the object last.
            for pathDir in reversed(searchPaths):
                file = os.path.join(pathDir, configFile)
                if os.path.exists(file):
                    self.filesRead.append(file)
                    self._updateWithOtherConfigFile(file)

    def _updateWithOtherConfigFile(self, file):
        """Read in some defaults and update.

        Update the configuration by reading the supplied file as a config
        of this class, and merging such that these values override the
        current values. Contents of the external config are not validated.

        Parameters
        ----------
        file : `Config`, `str`, or `dict`
            Entity that can be converted to a `ConfigSubset`.
        """
        # Use this class to read the defaults so that subsetting can happen
        # correctly.
        externalConfig = type(self)(file, validate=False, mergeDefaults=False)
        self.update(externalConfig)

    def validate(self):
        """Check that mandatory keys are present in this configuration.

        Ignored if ``requiredKeys`` is empty."""
        # Validation
        missing = [k for k in self.requiredKeys if k not in self.data]
        if missing:
            raise KeyError(f"Mandatory keys ({missing}) missing from supplied configuration for {type(self)}")
