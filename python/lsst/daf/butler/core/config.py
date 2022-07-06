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

from __future__ import annotations

"""Configuration control."""

__all__ = ("Config", "ConfigSubset")

import collections
import copy
import io
import json
import logging
import os
import pprint
import sys
from pathlib import Path
from typing import IO, Any, ClassVar, Dict, List, Optional, Sequence, Tuple, Union

import yaml
from lsst.resources import ResourcePath, ResourcePathExpression
from lsst.utils import doImport
from yaml.representer import Representer

yaml.add_representer(collections.defaultdict, Representer.represent_dict)


# Config module logger
log = logging.getLogger(__name__)

# PATH-like environment variable to use for defaults.
CONFIG_PATH = "DAF_BUTLER_CONFIG_PATH"

try:
    yamlLoader = yaml.CSafeLoader
except AttributeError:
    # Not all installations have the C library
    # (but assume for mypy's sake that they're the same)
    yamlLoader = yaml.SafeLoader  # type: ignore


def _doUpdate(d, u):
    if not isinstance(u, collections.abc.Mapping) or not isinstance(d, collections.abc.MutableMapping):
        raise RuntimeError("Only call update with Mapping, not {}".format(type(d)))
    for k, v in u.items():
        if isinstance(v, collections.abc.Mapping):
            d[k] = _doUpdate(d.get(k, {}), v)
        else:
            d[k] = v
    return d


def _checkNextItem(k, d, create, must_be_dict):
    """See if k is in d and if it is return the new child."""
    nextVal = None
    isThere = False
    if d is None:
        # We have gone past the end of the hierarchy
        pass
    elif not must_be_dict and isinstance(d, collections.abc.Sequence):
        # Check for Sequence first because for lists
        # __contains__ checks whether value is found in list
        # not whether the index exists in list. When we traverse
        # the hierarchy we are interested in the index.
        try:
            nextVal = d[int(k)]
            isThere = True
        except IndexError:
            pass
        except ValueError:
            isThere = k in d
    elif k in d:
        nextVal = d[k]
        isThere = True
    elif create:
        d[k] = {}
        nextVal = d[k]
        isThere = True

    return nextVal, isThere


class Loader(yamlLoader):
    """YAML Loader that supports file include directives.

    Uses ``!include`` directive in a YAML file to point to another
    YAML file to be included. The path in the include directive is relative
    to the file containing that directive.

        storageClasses: !include storageClasses.yaml

    Examples
    --------
    >>> with open("document.yaml", "r") as f:
           data = yaml.load(f, Loader=Loader)

    Notes
    -----
    See https://davidchall.github.io/yaml-includes.html
    """

    def __init__(self, stream):
        super().__init__(stream)
        # if this is a string and not a stream we may well lack a name
        try:
            self._root = ResourcePath(stream.name)
        except AttributeError:
            # No choice but to assume a local filesystem
            self._root = ResourcePath("no-file.yaml")
        Loader.add_constructor("!include", Loader.include)

    def include(self, node):
        result: Union[List[Any], Dict[str, Any]]
        if isinstance(node, yaml.ScalarNode):
            return self.extractFile(self.construct_scalar(node))

        elif isinstance(node, yaml.SequenceNode):
            result = []
            for filename in self.construct_sequence(node):
                result.append(self.extractFile(filename))
            return result

        elif isinstance(node, yaml.MappingNode):
            result = {}
            for k, v in self.construct_mapping(node).items():
                result[k] = self.extractFile(v)
            return result

        else:
            print("Error:: unrecognised node type in !include statement", file=sys.stderr)
            raise yaml.constructor.ConstructorError

    def extractFile(self, filename):
        # It is possible for the !include to point to an explicit URI
        # instead of a relative URI, therefore we first see if it is
        # scheme-less or not. If it has a scheme we use it directly
        # if it is scheme-less we use it relative to the file root.
        requesteduri = ResourcePath(filename, forceAbsolute=False)

        if requesteduri.scheme:
            fileuri = requesteduri
        else:
            fileuri = self._root.updatedFile(filename)

        log.debug("Opening YAML file via !include: %s", fileuri)

        # Read all the data from the resource
        data = fileuri.read()

        # Store the bytes into a BytesIO so we can attach a .name
        stream = io.BytesIO(data)
        stream.name = fileuri.geturl()
        return yaml.load(stream, Loader)


class Config(collections.abc.MutableMapping):
    r"""Implements a datatype that is used by `Butler` for configuration.

    It is essentially a `dict` with key/value pairs, including nested dicts
    (as values). In fact, it can be initialized with a `dict`.
    This is explained next:

    Config extends the `dict` api so that hierarchical values may be accessed
    with delimited notation or as a tuple.  If a string is given the delimiter
    is picked up from the first character in that string. For example,
    ``foo.getValue(".a.b.c")``, ``foo["a"]["b"]["c"]``, ``foo["a", "b", "c"]``,
    ``foo[".a.b.c"]``, and ``foo["/a/b/c"]`` all achieve the same outcome.
    If the first character is alphanumeric, no delimiter will be used.
    ``foo["a.b.c"]`` will be a single key ``a.b.c`` as will ``foo[":a.b.c"]``.
    Unicode characters can be used as the delimiter for distinctiveness if
    required.

    If a key in the hierarchy starts with a non-alphanumeric character care
    should be used to ensure that either the tuple interface is used or
    a distinct delimiter is always given in string form.

    Finally, the delimiter can be escaped if it is part of a key and also
    has to be used as a delimiter. For example, ``foo[r".a.b\.c"]`` results in
    a two element hierarchy of ``a`` and ``b.c``.  For hard-coded strings it is
    always better to use a different delimiter in these cases.

    Note that adding a multi-level key implicitly creates any nesting levels
    that do not exist, but removing multi-level keys does not automatically
    remove empty nesting levels.  As a result:

        >>> c = Config()
        >>> c[".a.b"] = 1
        >>> del c[".a.b"]
        >>> c["a"]
        Config({'a': {}})

    Storage formats supported:

    - yaml: read and write is supported.
    - json: read and write is supported but no ``!include`` directive.

    Parameters
    ----------
    other : `lsst.resources.ResourcePath` or `Config` or `dict`
        Other source of configuration, can be:

        - (`lsst.resources.ResourcePathExpression`)
          Treated as a URI to a config file. Must end with ".yaml".
        - (`Config`) Copies the other Config's values into this one.
        - (`dict`) Copies the values from the dict into this Config.

        If `None` is provided an empty `Config` will be created.
    """

    _D: str = "→"
    """Default internal delimiter to use for components in the hierarchy when
    constructing keys for external use (see `Config.names()`)."""

    includeKey: ClassVar[str] = "includeConfigs"
    """Key used to indicate that another config should be included at this
    part of the hierarchy."""

    resourcesPackage: str = "lsst.daf.butler"
    """Package to search for default configuration data.  The resources
    themselves will be within a ``configs`` resource hierarchy."""

    def __init__(self, other=None):
        self._data: Dict[str, Any] = {}
        self.configFile = None

        if other is None:
            return

        if isinstance(other, Config):
            self._data = copy.deepcopy(other._data)
            self.configFile = other.configFile
        elif isinstance(other, (dict, collections.abc.Mapping)):
            # In most cases we have a dict, and it's more efficient
            # to check for a dict instance before checking the generic mapping.
            self.update(other)
        elif isinstance(other, (str, ResourcePath, Path)):
            # if other is a string, assume it is a file path/URI
            self.__initFromUri(other)
            self._processExplicitIncludes()
        else:
            # if the config specified by other could not be recognized raise
            # a runtime error.
            raise RuntimeError(f"A Config could not be loaded from other: {other}")

    def ppprint(self):
        """Return config as formatted readable string.

        Examples
        --------
        use: ``pdb> print(myConfigObject.ppprint())``

        Returns
        -------
        s : `str`
            A prettyprint formatted string representing the config
        """
        return pprint.pformat(self._data, indent=2, width=1)

    def __repr__(self):
        return f"{type(self).__name__}({self._data!r})"

    def __str__(self):
        return self.ppprint()

    def __len__(self):
        return len(self._data)

    def __iter__(self):
        return iter(self._data)

    def copy(self):
        return type(self)(self)

    @classmethod
    def fromString(cls, string: str, format: str = "yaml") -> Config:
        """Create a new Config instance from a serialized string.

        Parameters
        ----------
        string : `str`
            String containing content in specified format
        format : `str`, optional
            Format of the supplied string. Can be ``json`` or ``yaml``.

        Returns
        -------
        c : `Config`
            Newly-constructed Config.
        """
        if format == "yaml":
            new_config = cls().__initFromYaml(string)
        elif format == "json":
            new_config = cls().__initFromJson(string)
        else:
            raise ValueError(f"Unexpected format of string: {format}")
        new_config._processExplicitIncludes()
        return new_config

    @classmethod
    def fromYaml(cls, string: str) -> Config:
        """Create a new Config instance from a YAML string.

        Parameters
        ----------
        string : `str`
            String containing content in YAML format

        Returns
        -------
        c : `Config`
            Newly-constructed Config.
        """
        return cls.fromString(string, format="yaml")

    def __initFromUri(self, path: ResourcePathExpression) -> None:
        """Load a file from a path or an URI.

        Parameters
        ----------
        path : `lsst.resources.ResourcePathExpression`
            Path or a URI to a persisted config file.
        """
        uri = ResourcePath(path)
        ext = uri.getExtension()
        if ext == ".yaml":
            log.debug("Opening YAML config file: %s", uri.geturl())
            content = uri.read()
            # Use a stream so we can name it
            stream = io.BytesIO(content)
            stream.name = uri.geturl()
            self.__initFromYaml(stream)
        elif ext == ".json":
            log.debug("Opening JSON config file: %s", uri.geturl())
            content = uri.read()
            self.__initFromJson(content)
        else:
            # This URI does not have a valid extension. It might be because
            # we ended up with a directory and not a file. Before we complain
            # about an extension, do an existence check.  No need to do
            # the (possibly expensive) existence check in the default code
            # path above because we will find out soon enough that the file
            # is not there.
            if not uri.exists():
                raise FileNotFoundError(f"Config location {uri} does not exist.")
            raise RuntimeError(f"The Config URI does not have a supported extension: {uri}")
        self.configFile = uri

    def __initFromYaml(self, stream):
        """Load a YAML config from any readable stream that contains one.

        Parameters
        ----------
        stream: `IO` or `str`
            Stream to pass to the YAML loader. Accepts anything that
            `yaml.load` accepts.  This can include a string as well as an
            IO stream.

        Raises
        ------
        yaml.YAMLError
            If there is an error loading the file.
        """
        content = yaml.load(stream, Loader=Loader)
        if content is None:
            content = {}
        self._data = content
        return self

    def __initFromJson(self, stream):
        """Load a JSON config from any readable stream that contains one.

        Parameters
        ----------
        stream: `IO` or `str`
            Stream to pass to the JSON loader. This can include a string as
            well as an IO stream.

        Raises
        ------
        TypeError:
            Raised if there is an error loading the content.
        """
        if isinstance(stream, (bytes, str)):
            content = json.loads(stream)
        else:
            content = json.load(stream)
        if content is None:
            content = {}
        self._data = content
        return self

    def _processExplicitIncludes(self):
        """Scan through the configuration searching for the special includes.

        Looks for ``includeConfigs`` directive and processes the includes.
        """
        # Search paths for config files
        searchPaths = [ResourcePath(os.path.curdir, forceDirectory=True)]
        if self.configFile is not None:
            if isinstance(self.configFile, ResourcePath):
                configDir = self.configFile.dirname()
            else:
                raise RuntimeError(f"Unexpected type for config file: {self.configFile}")
            searchPaths.append(configDir)

        # Ensure we know what delimiter to use
        names = self.nameTuples()
        for path in names:
            if path[-1] == self.includeKey:

                log.debug("Processing file include directive at %s", self._D + self._D.join(path))
                basePath = path[:-1]

                # Extract the includes and then delete them from the config
                includes = self[path]
                del self[path]

                # Be consistent and convert to a list
                if not isinstance(includes, list):
                    includes = [includes]

                # Read each file assuming it is a reference to a file
                # The file can be relative to config file or cwd
                # ConfigSubset search paths are not used
                subConfigs = []
                for fileName in includes:
                    # Expand any shell variables -- this could be URI
                    fileName = ResourcePath(os.path.expandvars(fileName), forceAbsolute=False)
                    found = None
                    if fileName.isabs():
                        found = fileName
                    else:
                        for dir in searchPaths:
                            if isinstance(dir, ResourcePath):
                                specific = dir.join(fileName.path)
                                # Remote resource check might be expensive
                                if specific.exists():
                                    found = specific
                            else:
                                log.warning(
                                    "Do not understand search path entry '%s' of type %s",
                                    dir,
                                    type(dir).__name__,
                                )
                    if not found:
                        raise RuntimeError(f"Unable to find referenced include file: {fileName}")

                    # Read the referenced Config as a Config
                    subConfigs.append(type(self)(found))

                # Now we need to merge these sub configs with the current
                # information that was present in this node in the config
                # tree with precedence given to the explicit values
                newConfig = subConfigs.pop(0)
                for sc in subConfigs:
                    newConfig.update(sc)

                # Explicit values take precedence
                if not basePath:
                    # This is an include at the root config
                    newConfig.update(self)
                    # Replace the current config
                    self._data = newConfig._data
                else:
                    newConfig.update(self[basePath])
                    # And reattach to the base config
                    self[basePath] = newConfig

    @staticmethod
    def _splitIntoKeys(key):
        r"""Split the argument for get/set/in into a hierarchical list.

        Parameters
        ----------
        key : `str` or iterable
            Argument given to get/set/in. If an iterable is provided it will
            be converted to a list.  If the first character of the string
            is not an alphanumeric character then it will be used as the
            delimiter for the purposes of splitting the remainder of the
            string. If the delimiter is also in one of the keys then it
            can be escaped using ``\``. There is no default delimiter.

        Returns
        -------
        keys : `list`
            Hierarchical keys as a `list`.
        """
        if isinstance(key, str):
            if not key[0].isalnum():
                d = key[0]
                key = key[1:]
            else:
                return [
                    key,
                ]
            escaped = f"\\{d}"
            temp = None
            if escaped in key:
                # Complain at the attempt to escape the escape
                doubled = rf"\{escaped}"
                if doubled in key:
                    raise ValueError(
                        f"Escaping an escaped delimiter ({doubled} in {key}) is not yet supported."
                    )
                # Replace with a character that won't be in the string
                temp = "\r"
                if temp in key or d == temp:
                    raise ValueError(
                        f"Can not use character {temp!r} in hierarchical key or as"
                        " delimiter if escaping the delimiter"
                    )
                key = key.replace(escaped, temp)
            hierarchy = key.split(d)
            if temp:
                hierarchy = [h.replace(temp, d) for h in hierarchy]
            return hierarchy
        elif isinstance(key, collections.abc.Iterable):
            return list(key)
        else:
            # Not sure what this is so try it anyway
            return [
                key,
            ]

    def _getKeyHierarchy(self, name):
        """Retrieve the key hierarchy for accessing the Config.

        Parameters
        ----------
        name : `str` or `tuple`
            Delimited string or `tuple` of hierarchical keys.

        Returns
        -------
        hierarchy : `list` of `str`
            Hierarchy to use as a `list`.  If the name is available directly
            as a key in the Config it will be used regardless of the presence
            of any nominal delimiter.
        """
        if name in self._data:
            keys = [
                name,
            ]
        else:
            keys = self._splitIntoKeys(name)
        return keys

    def _findInHierarchy(self, keys, create=False):
        """Look for hierarchy of keys in Config.

        Parameters
        ----------
        keys : `list` or `tuple`
            Keys to search in hierarchy.
        create : `bool`, optional
            If `True`, if a part of the hierarchy does not exist, insert an
            empty `dict` into the hierarchy.

        Returns
        -------
        hierarchy : `list`
            List of the value corresponding to each key in the supplied
            hierarchy.  Only keys that exist in the hierarchy will have
            a value.
        complete : `bool`
            `True` if the full hierarchy exists and the final element
            in ``hierarchy`` is the value of relevant value.
        """
        d = self._data

        # For the first key, d must be a dict so it is a waste
        # of time to check for a sequence.
        must_be_dict = True

        hierarchy = []
        complete = True
        for k in keys:
            d, isThere = _checkNextItem(k, d, create, must_be_dict)
            if isThere:
                hierarchy.append(d)
            else:
                complete = False
                break
            # Second time round it might be a sequence.
            must_be_dict = False

        return hierarchy, complete

    def __getitem__(self, name):
        # Override the split for the simple case where there is an exact
        # match.  This allows `Config.items()` to work via a simple
        # __iter__ implementation that returns top level keys of
        # self._data.

        # If the name matches a key in the top-level hierarchy, bypass
        # all further cleverness.
        found_directly = False
        try:
            data = self._data[name]
            found_directly = True
        except KeyError:
            pass

        if not found_directly:
            keys = self._getKeyHierarchy(name)

            hierarchy, complete = self._findInHierarchy(keys)
            if not complete:
                raise KeyError(f"{name} not found")
            data = hierarchy[-1]

        # In most cases we have a dict, and it's more efficient
        # to check for a dict instance before checking the generic mapping.
        if isinstance(data, (dict, collections.abc.Mapping)):
            data = Config(data)
            # Ensure that child configs inherit the parent internal delimiter
            if self._D != Config._D:
                data._D = self._D
        return data

    def __setitem__(self, name, value):
        keys = self._getKeyHierarchy(name)
        last = keys.pop()
        if isinstance(value, Config):
            value = copy.deepcopy(value._data)

        hierarchy, complete = self._findInHierarchy(keys, create=True)
        if hierarchy:
            data = hierarchy[-1]
        else:
            data = self._data

        try:
            data[last] = value
        except TypeError:
            data[int(last)] = value

    def __contains__(self, key):
        keys = self._getKeyHierarchy(key)
        hierarchy, complete = self._findInHierarchy(keys)
        return complete

    def __delitem__(self, key):
        keys = self._getKeyHierarchy(key)
        last = keys.pop()
        hierarchy, complete = self._findInHierarchy(keys)
        if complete:
            if hierarchy:
                data = hierarchy[-1]
            else:
                data = self._data
            del data[last]
        else:
            raise KeyError(f"{key} not found in Config")

    def update(self, other):
        """Update config from other `Config` or `dict`.

        Like `dict.update()`, but will add or modify keys in nested dicts,
        instead of overwriting the nested dict entirely.

        Parameters
        ----------
        other : `dict` or `Config`
            Source of configuration:

        Examples
        --------
        >>> c = Config({"a": {"b": 1}})
        >>> c.update({"a": {"c": 2}})
        >>> print(c)
        {'a': {'b': 1, 'c': 2}}

        >>> foo = {"a": {"b": 1}}
        >>> foo.update({"a": {"c": 2}})
        >>> print(foo)
        {'a': {'c': 2}}
        """
        _doUpdate(self._data, other)

    def merge(self, other):
        """Merge another Config into this one.

        Like `Config.update()`, but will add keys & values from other that
        DO NOT EXIST in self.

        Keys and values that already exist in self will NOT be overwritten.

        Parameters
        ----------
        other : `dict` or `Config`
            Source of configuration:
        """
        if not isinstance(other, collections.abc.Mapping):
            raise TypeError(f"Can only merge a Mapping into a Config, not {type(other)}")

        # Convert the supplied mapping to a Config for consistency
        # This will do a deepcopy if it is already a Config
        otherCopy = Config(other)
        otherCopy.update(self)
        self._data = otherCopy._data

    def nameTuples(self, topLevelOnly=False):
        """Get tuples representing the name hierarchies of all keys.

        The tuples returned from this method are guaranteed to be usable
        to access items in the configuration object.

        Parameters
        ----------
        topLevelOnly : `bool`, optional
            If False, the default, a full hierarchy of names is returned.
            If True, only the top level are returned.

        Returns
        -------
        names : `list` of `tuple` of `str`
            List of all names present in the `Config` where each element
            in the list is a `tuple` of strings representing the hierarchy.
        """
        if topLevelOnly:
            return list((k,) for k in self)

        def getKeysAsTuples(d, keys, base):
            if isinstance(d, collections.abc.Sequence):
                theseKeys = range(len(d))
            else:
                theseKeys = d.keys()
            for key in theseKeys:
                val = d[key]
                levelKey = base + (key,) if base is not None else (key,)
                keys.append(levelKey)
                if isinstance(val, (collections.abc.Mapping, collections.abc.Sequence)) and not isinstance(
                    val, str
                ):
                    getKeysAsTuples(val, keys, levelKey)

        keys: List[Tuple[str, ...]] = []
        getKeysAsTuples(self._data, keys, None)
        return keys

    def names(self, topLevelOnly=False, delimiter=None):
        """Get a delimited name of all the keys in the hierarchy.

        The values returned from this method are guaranteed to be usable
        to access items in the configuration object.

        Parameters
        ----------
        topLevelOnly : `bool`, optional
            If False, the default, a full hierarchy of names is returned.
            If True, only the top level are returned.
        delimiter : `str`, optional
            Delimiter to use when forming the keys.  If the delimiter is
            present in any of the keys, it will be escaped in the returned
            names.  If `None` given a delimiter will be automatically provided.
            The delimiter can not be alphanumeric.

        Returns
        -------
        names : `list` of `str`
            List of all names present in the `Config`.

        Notes
        -----
        This is different than the built-in method `dict.keys`, which will
        return only the first level keys.

        Raises
        ------
        ValueError:
            The supplied delimiter is alphanumeric.
        """
        if topLevelOnly:
            return list(self.keys())

        # Get all the tuples of hierarchical keys
        nameTuples = self.nameTuples()

        if delimiter is not None and delimiter.isalnum():
            raise ValueError(f"Supplied delimiter ({delimiter!r}) must not be alphanumeric.")

        if delimiter is None:
            # Start with something, and ensure it does not need to be
            # escaped (it is much easier to understand if not escaped)
            delimiter = self._D

            # Form big string for easy check of delimiter clash
            combined = "".join("".join(str(s) for s in k) for k in nameTuples)

            # Try a delimiter and keep trying until we get something that
            # works.
            ntries = 0
            while delimiter in combined:
                log.debug("Delimiter '%s' could not be used. Trying another.", delimiter)
                ntries += 1

                if ntries > 100:
                    raise ValueError(f"Unable to determine a delimiter for Config {self}")

                # try another one
                while True:
                    delimiter = chr(ord(delimiter) + 1)
                    if not delimiter.isalnum():
                        break

        log.debug("Using delimiter %r", delimiter)

        # Form the keys, escaping the delimiter if necessary
        strings = [
            delimiter + delimiter.join(str(s).replace(delimiter, f"\\{delimiter}") for s in k)
            for k in nameTuples
        ]
        return strings

    def asArray(self, name):
        """Get a value as an array.

        May contain one or more elements.

        Parameters
        ----------
        name : `str`
            Key to use to retrieve value.

        Returns
        -------
        array : `collections.abc.Sequence`
            The value corresponding to name, but guaranteed to be returned
            as a list with at least one element. If the value is a
            `~collections.abc.Sequence` (and not a `str`) the value itself
            will be returned, else the value will be the first element.
        """
        val = self.get(name)
        if isinstance(val, str):
            val = [val]
        elif not isinstance(val, collections.abc.Sequence):
            val = [val]
        return val

    def __eq__(self, other):
        if isinstance(other, Config):
            other = other._data
        return self._data == other

    def __ne__(self, other):
        if isinstance(other, Config):
            other = other._data
        return self._data != other

    #######
    # i/o #

    def dump(self, output: Optional[IO] = None, format: str = "yaml") -> Optional[str]:
        """Write the config to an output stream.

        Parameters
        ----------
        output : `IO`, optional
            The stream to use for output. If `None` the serialized content
            will be returned.
        format : `str`, optional
            The format to use for the output. Can be "yaml" or "json".

        Returns
        -------
        serialized : `str` or `None`
            If a stream was given the stream will be used and the return
            value will be `None`. If the stream was `None` the
            serialization will be returned as a string.
        """
        if format == "yaml":
            return yaml.safe_dump(self._data, output, default_flow_style=False)
        elif format == "json":
            if output is not None:
                json.dump(self._data, output, ensure_ascii=False)
                return None
            else:
                return json.dumps(self._data, ensure_ascii=False)
        raise ValueError(f"Unsupported format for Config serialization: {format}")

    def dumpToUri(
        self,
        uri: ResourcePathExpression,
        updateFile: bool = True,
        defaultFileName: str = "butler.yaml",
        overwrite: bool = True,
    ) -> None:
        """Write the config to location pointed to by given URI.

        Currently supports 's3' and 'file' URI schemes.

        Parameters
        ----------
        uri: `lsst.resources.ResourcePathExpression`
            URI of location where the Config will be written.
        updateFile : bool, optional
            If True and uri does not end on a filename with extension, will
            append `defaultFileName` to the target uri. True by default.
        defaultFileName : bool, optional
            The file name that will be appended to target uri if updateFile is
            True and uri does not end on a file with an extension.
        overwrite : bool, optional
            If True the configuration will be written even if it already
            exists at that location.
        """
        # Make local copy of URI or create new one
        uri = ResourcePath(uri)

        if updateFile and not uri.getExtension():
            uri = uri.updatedFile(defaultFileName)

        # Try to work out the format from the extension
        ext = uri.getExtension()
        format = ext[1:].lower()

        output = self.dump(format=format)
        assert output is not None, "Config.dump guarantees not-None return when output arg is None"
        uri.write(output.encode(), overwrite=overwrite)
        self.configFile = uri

    @staticmethod
    def updateParameters(configType, config, full, toUpdate=None, toCopy=None, overwrite=True, toMerge=None):
        """Update specific config parameters.

        Allows for named parameters to be set to new values in bulk, and
        for other values to be set by copying from a reference config.

        Assumes that the supplied config is compatible with ``configType``
        and will attach the updated values to the supplied config by
        looking for the related component key.  It is assumed that
        ``config`` and ``full`` are from the same part of the
        configuration hierarchy.

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
            ``toCopy`` is defined.

            Repository-specific options that should not be obtained
            from defaults when Butler instances are constructed
            should be copied from ``full`` to ``config``.
        toUpdate : `dict`, optional
            A `dict` defining the keys to update and the new value to use.
            The keys and values can be any supported by `Config`
            assignment.
        toCopy : `tuple`, optional
            `tuple` of keys whose values should be copied from ``full``
            into ``config``.
        overwrite : `bool`, optional
            If `False`, do not modify a value in ``config`` if the key
            already exists.  Default is always to overwrite.
        toMerge : `tuple`, optional
            Keys to merge content from full to config without overwriting
            pre-existing values. Only works if the key refers to a hierarchy.
            The ``overwrite`` flag is ignored.

        Raises
        ------
        ValueError
            Neither ``toUpdate``, ``toCopy`` nor ``toMerge`` were defined.
        """
        if toUpdate is None and toCopy is None and toMerge is None:
            raise ValueError("At least one of toUpdate, toCopy, or toMerge parameters must be set.")

        # If this is a parent configuration then we need to ensure that
        # the supplied config has the relevant component key in it.
        # If this is a parent configuration we add in the stub entry
        # so that the ConfigSubset constructor will do the right thing.
        # We check full for this since that is guaranteed to be complete.
        if configType.component in full and configType.component not in config:
            config[configType.component] = {}

        # Extract the part of the config we wish to update
        localConfig = configType(config, mergeDefaults=False, validate=False)

        if toUpdate:
            for key, value in toUpdate.items():
                if key in localConfig and not overwrite:
                    log.debug(
                        "Not overriding key '%s' with value '%s' in config %s",
                        key,
                        value,
                        localConfig.__class__.__name__,
                    )
                else:
                    localConfig[key] = value

        if toCopy or toMerge:
            localFullConfig = configType(full, mergeDefaults=False)

            if toCopy:
                for key in toCopy:
                    if key in localConfig and not overwrite:
                        log.debug(
                            "Not overriding key '%s' from defaults in config %s",
                            key,
                            localConfig.__class__.__name__,
                        )
                    else:
                        localConfig[key] = localFullConfig[key]
            if toMerge:
                for key in toMerge:
                    if key in localConfig:
                        # Get the node from the config to do the merge
                        # but then have to reattach to the config.
                        subset = localConfig[key]
                        subset.merge(localFullConfig[key])
                        localConfig[key] = subset
                    else:
                        localConfig[key] = localFullConfig[key]

        # Reattach to parent if this is a child config
        if configType.component in config:
            config[configType.component] = localConfig
        else:
            config.update(localConfig)

    def toDict(self):
        """Convert a `Config` to a standalone hierarchical `dict`.

        Returns
        -------
        d : `dict`
            The standalone hierarchical `dict` with any `Config` classes
            in the hierarchy converted to `dict`.

        Notes
        -----
        This can be useful when passing a Config to some code that
        expects native Python types.
        """
        output = copy.deepcopy(self._data)
        for k, v in output.items():
            if isinstance(v, Config):
                v = v.toDict()
            output[k] = v
        return output


class ConfigSubset(Config):
    """Config representing a subset of a more general configuration.

    Subclasses define their own component and when given a configuration
    that includes that component, the resulting configuration only includes
    the subset.  For example, your config might contain ``dimensions`` if it's
    part of a global config and that subset will be stored. If ``dimensions``
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
        be combined with the defaults, with the supplied values taking
        precedence.
    searchPaths : `list` or `tuple`, optional
        Explicit additional paths to search for defaults. They should
        be supplied in priority order. These paths have higher priority
        than those read from the environment in
        `ConfigSubset.defaultSearchPaths()`.  Paths can be `str` referring to
        the local file system or URIs, `lsst.resources.ResourcePath`.
    """

    component: ClassVar[Optional[str]] = None
    """Component to use from supplied config. Can be None. If specified the
    key is not required. Can be a full dot-separated path to a component.
    """

    requiredKeys: ClassVar[Sequence[str]] = ()
    """Keys that are required to be specified in the configuration.
    """

    defaultConfigFile: ClassVar[Optional[str]] = None
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
            doubled = (self.component, self.component)
            # Must check for double depth first
            if doubled in externalConfig:
                externalConfig = externalConfig[doubled]
            elif self.component in externalConfig:
                externalConfig._data = externalConfig._data[self.component]

        # Default files read to create this configuration
        self.filesRead = []

        # Assume we are not looking up child configurations
        containerKey = None

        # Sometimes we do not want to merge with defaults.
        if mergeDefaults:

            # Supplied search paths have highest priority
            fullSearchPath = []
            if searchPaths:
                fullSearchPath.extend(searchPaths)

            # Read default paths from environment
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
                except ImportError as e:
                    raise RuntimeError(f"Failed to import cls '{pytype}' for config {type(self)}") from e
                defaultsFile = cls.defaultConfigFile
                if defaultsFile is not None:
                    self._updateWithConfigsFromPath(fullSearchPath, defaultsFile)

                # Get the container key in case we need it
                try:
                    containerKey = cls.containerKey
                except AttributeError:
                    pass

        # Now update this object with the external values so that the external
        # values always override the defaults
        self.update(externalConfig)
        if not self.configFile:
            self.configFile = externalConfig.configFile

        # If this configuration has child configurations of the same
        # config class, we need to expand those defaults as well.

        if mergeDefaults and containerKey is not None and containerKey in self:
            for idx, subConfig in enumerate(self[containerKey]):
                self[containerKey, idx] = type(self)(
                    other=subConfig, validate=validate, mergeDefaults=mergeDefaults, searchPaths=searchPaths
                )

        if validate:
            self.validate()

    @classmethod
    def defaultSearchPaths(cls):
        """Read environment to determine search paths to use.

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
            configuration resources will not be included here but will
            always be searched last.

        Notes
        -----
        The environment variable is split on the standard ``:`` path separator.
        This currently makes it incompatible with usage of URIs.
        """
        # We can pick up defaults from multiple search paths
        # We fill defaults by using the butler config path and then
        # the config path environment variable in reverse order.
        defaultsPaths: List[Union[str, ResourcePath]] = []

        if CONFIG_PATH in os.environ:
            externalPaths = os.environ[CONFIG_PATH].split(os.pathsep)
            defaultsPaths.extend(externalPaths)

        # Add the package defaults as a resource
        defaultsPaths.append(ResourcePath(f"resource://{cls.resourcesPackage}/configs", forceDirectory=True))
        return defaultsPaths

    def _updateWithConfigsFromPath(self, searchPaths, configFile):
        """Search the supplied paths, merging the configuration values.

        The values read will override values currently stored in the object.
        Every file found in the path will be read, such that the earlier
        path entries have higher priority.

        Parameters
        ----------
        searchPaths : `list` of `lsst.resources.ResourcePath`, `str`
            Paths to search for the supplied configFile. This path
            is the priority order, such that files read from the
            first path entry will be selected over those read from
            a later path. Can contain `str` referring to the local file
            system or a URI string.
        configFile : `lsst.resources.ResourcePath`
            File to locate in path. If absolute path it will be read
            directly and the search path will not be used. Can be a URI
            to an explicit resource (which will ignore the search path)
            which is assumed to exist.
        """
        uri = ResourcePath(configFile)
        if uri.isabs() and uri.exists():
            # Assume this resource exists
            self._updateWithOtherConfigFile(configFile)
            self.filesRead.append(configFile)
        else:
            # Reverse order so that high priority entries
            # update the object last.
            for pathDir in reversed(searchPaths):
                if isinstance(pathDir, (str, ResourcePath)):
                    pathDir = ResourcePath(pathDir, forceDirectory=True)
                    file = pathDir.join(configFile)
                    if file.exists():
                        self.filesRead.append(file)
                        self._updateWithOtherConfigFile(file)
                else:
                    raise ValueError(f"Unexpected search path type encountered: {pathDir!r}")

    def _updateWithOtherConfigFile(self, file):
        """Read in some defaults and update.

        Update the configuration by reading the supplied file as a config
        of this class, and merging such that these values override the
        current values. Contents of the external config are not validated.

        Parameters
        ----------
        file : `Config`, `str`, `lsst.resources.ResourcePath`, or `dict`
            Entity that can be converted to a `ConfigSubset`.
        """
        # Use this class to read the defaults so that subsetting can happen
        # correctly.
        externalConfig = type(self)(file, validate=False, mergeDefaults=False)
        self.update(externalConfig)

    def validate(self):
        """Check that mandatory keys are present in this configuration.

        Ignored if ``requiredKeys`` is empty.
        """
        # Validation
        missing = [k for k in self.requiredKeys if k not in self._data]
        if missing:
            raise KeyError(f"Mandatory keys ({missing}) missing from supplied configuration for {type(self)}")
