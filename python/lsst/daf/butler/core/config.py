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

import collections
import copy
import yaml
import pprint

from yaml.representer import Representer
yaml.add_representer(collections.defaultdict, Representer.represent_dict)


# UserDict and yaml have defined metaclasses and Python 3 does not allow multiple
# inheritance of classes with distinct metaclasses. We therefore have to
# create a new baseclass that Config can inherit from. This is because the metaclass
# syntax differs between versions

class _ConfigMeta(type(collections.UserDict), type(yaml.YAMLObject)):
    pass


class _ConfigBase(collections.UserDict, yaml.YAMLObject, metaclass=_ConfigMeta):
    pass


class Config(_ConfigBase):
    """Config implements a datatype that is used by Butler for configuration parameters.
    It is essentially a dict with key/value pairs, including nested dicts (as values). In fact, it can be
    initialized with a dict. The only caveat is that keys may NOT contain dots ('.'). This is explained next:
    Config extends the dict api so that hierarchical values may be accessed with dot-delimited notation.
    That is, foo.getValue('a.b.c') is the same as foo['a']['b']['c'] is the same as foo['a.b.c'], and either
    of these syntaxes may be used.

    Storage formats supported:
    - yaml: read and write is supported.
    """

    def __init__(self, other=None):
        """Initialize the Config. Other can be used to initialize the Config in a variety of ways.

        Parameters
        ----------
        other: `str` or `Config` or `dict`
            Other source of configuration, can be:

            - (`str`) Treated as a path to a config file on disk. Must end with '.yaml'.
            - (`Config`) Copies the other Config's values into this one.
            - (`dict`) Copies the values from the dict into this Config.
        """
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
        """helper function for debugging, prints a config out in a readable way in the debugger.

        use: pdb> print myConfigObject.pprint()

        Returns
        -------
        s: `str`
            A prettyprint formatted string representing the config
        """
        return pprint.pformat(self.data, indent=2, width=1)

    def __repr__(self):
        return self.data.__repr__()

    def __initFromFile(self, path):
        """Load a file from path.

        Parameters
        ----------
        path: `str`
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
        path: `str`
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
        e: `yaml.YAMLError`
            If there is an error loading the file.
        """
        self.data = yaml.safe_load(stream)
        return self

    def __getitem__(self, name):
        data = self.data
        for key in name.split('.'):
            if data is None:
                return None
            if key in data:
                data = data[key]
            else:
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
            data = data.setdefault(key, {})
        data[last] = value

    def __contains__(self, key):
        d = self.data
        keys = key.split('.')
        last = keys.pop()
        for k in keys:
            if k in d:
                d = d[k]
            else:
                return False
        return last in d

    def update(self, other):
        """Like dict.update, but will add or modify keys in nested dicts, instead of overwriting the nested
        dict entirely.

        For example, for the given code:
        foo = {'a': {'b': 1}}
        foo.update({'a': {'c': 2}})

        Parameters
        ----------
        other: `dict` or `Config`
            Source of configuration:

            - If foo is a dict, then after the update foo == {'a': {'c': 2}}
            - But if foo is a Config, then after the update foo == {'a': {'b': 1, 'c': 2}}
        """
        def doUpdate(d, u):
            for k, v in u.items():
                if isinstance(d, collections.Mapping):
                    if isinstance(v, collections.Mapping):
                        r = doUpdate(d.get(k, {}), v)
                        d[k] = r
                    else:
                        d[k] = u[k]
                else:
                    d = {k: u[k]}
            return d
        doUpdate(self.data, other)

    def merge(self, other):
        """Like Config.update, but will add keys & values from other that DO NOT EXIST in self. Keys and
        values that already exist in self will NOT be overwritten.

        Parameters
        ----------
        other: `dict` or `Config`
            Source of configuration:
        """
        otherCopy = copy.deepcopy(other)
        otherCopy.update(self)
        self.data = otherCopy.data

    def names(self, topLevelOnly=False):
        """Get the dot-delimited name of all the keys in the hierarchy.
        NOTE: this is different than the built-in method dict.keys, which will return only the first level
        keys.
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
        name: `str`
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
        # First a set of known keys is handled and written to the stream in a specific order for readability.
        # After the expected/ordered keys are weritten to the stream the remainder of the keys are written to
        # the stream.
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
        path: `str`
            Path to the file to use for output.
        """
        with open(path, 'w') as f:
            self.dump(f)
