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

"""Support for configuration snippets"""

__all__ = ("LookupKey", "processLookupConfigs",
           "processLookupConfigList")

import logging
import re
from collections.abc import Mapping
from .dimensions import DimensionGraph

log = logging.getLogger(__name__)

DATAID_RE = re.compile(r"([a-z_]+)<(.*)>$")
"""Regex to find dataIds embedded in configurations."""


class LookupKey:
    """Representation of key that can be used to lookup information based
    on dataset type name, storage class name, dimensions.

    Parameters
    ----------
    name : `str`, optional
        Primary index string for lookup.  If this string looks like it
        represents dimensions (via ``dim1+dim2+dim3`` syntax) the name
        is converted to a `DimensionNameSet` and stored in ``dimensions``
        property.
    dimensions : `DimensionNameSet` or `DimensionGraph`, optional
        Dimensions that are relevant for lookup. Should not be specified
        if ``name`` is also specified.
    dataId : `dict`, optional
        Keys and values from a dataId that should control lookups.
    universe : `DimensionUniverse`, optional
        Set of all known dimensions, used to expand and validate ``name`` or
        ``dimensions``.  Required if the key represents dimensions and a
        full `DimensionGraph` is not provided.
    """

    def __init__(self, name=None, dimensions=None, dataId=None, *, universe=None):
        if name is None and dimensions is None:
            raise ValueError("At least one of name or dimensions must be given")

        if name is not None and dimensions is not None:
            raise ValueError("Can only accept one of name or dimensions")

        self._dimensions = None
        self._name = None

        if name is not None:

            if not isinstance(name, str):
                raise ValueError(f"Supplied name must be str not: '{name}'")

            if "+" in name:
                # If we are given a single dimension we use the "+" to
                # indicate this but have to filter out the empty value
                dimensions = [n for n in name.split("+") if n]
                if universe is None:
                    raise ValueError(f"Cannot construct LookupKey for {name} without dimension universe.")
                else:
                    self._dimensions = universe.extract(dimensions)
            else:
                self._name = name
        else:
            if not isinstance(dimensions, DimensionGraph):
                if universe is None:
                    raise ValueError(f"Cannot construct LookupKey for dimensions={dimensions} "
                                     f"without universe.")
                else:
                    self._dimensions = universe.extract(dimensions)
            else:
                self._dimensions = dimensions

        # The dataId is converted to a frozenset of key/value
        # tuples so that it is not mutable
        if dataId is not None:
            self._dataId = frozenset(dataId.items())
        else:
            self._dataId = None

    def __str__(self):
        # For the simple case return the simple string
        if self._name:
            name = self._name
        else:
            name = "+".join(self._dimensions.names)

        if not self._dataId:
            return name

        return f"{name} ({self.dataId})"

    def __repr__(self):
        params = ""
        if self.name:
            params += f"name={self.name!r},"
        if self.dimensions:
            params += f"dimensions={self.dimensions!r},"
        if self._dataId:
            params += "dataId={" + ",".join(f"'{k}': {v!r}" for k, v in self._dataId) + "}"

        return f"{self.__class__.__name__}({params})"

    def __eq__(self, other):
        if self._name == other._name and self._dimensions == other._dimensions and \
                self._dataId == other._dataId:
            return True
        return False

    @property
    def name(self):
        """Primary name string to use as lookup. (`str`)"""
        return self._name

    @property
    def dimensions(self):
        """Dimensions associated with lookup.
        (`DimensionGraph` or `DimensionNameSet`)"""
        return self._dimensions

    @property
    def dataId(self):
        """Dict of keys/values that are important for dataId lookup.
        (`dict` or `None`)"""
        if self._dataId is not None:
            return {k: v for k, v in self._dataId}
        else:
            return

    def __hash__(self):
        """Hash the lookup to allow use as a key in a dict."""
        return hash((self._name, self._dimensions, self._dataId))

    def clone(self, name=None, dimensions=None, dataId=None):
        """Clone the object, overriding some options.

        Used to create a new instance of the object whilst updating
        some of it.

        Parameters
        ----------
        name : `str`, optional
            Primary index string for lookup.  Will override ``dimensions``
            if ``dimensions`` are set.
        dimensions : `DimensionNameSet`, optional
            Dimensions that are relevant for lookup. Will override ``name``
            if ``name`` is already set.
        dataId : `dict`, optional
            Keys and values from a dataId that should control lookups.

        Returns
        -------
        clone : `LookupKey`
            Copy with updates.
        """
        if name is not None and dimensions is not None:
            raise ValueError("Both name and dimensions can not be set")

        # if neither name nor dimensions are specified we copy from current
        # object. Otherwise we'll use the supplied values
        if name is None and dimensions is None:
            name = self._name
            dimensions = self._dimensions

        # Make sure we use the dict form for the constructor
        if dataId is None and self._dataId is not None:
            dataId = self.dataId

        return self.__class__(name=name, dimensions=dimensions, dataId=dataId)


def processLookupConfigs(config, *, universe=None):
    """Process sections of configuration relating to lookups by dataset type
    name, storage class name, dimensions, or values of dimensions.

    Parameters
    ----------
    config : `Config`
        A `Config` representing a configuration mapping keys to values where
        the keys can be dataset type names, storage class names, dimensions
        or dataId components.
    universe : `DimensionUniverse`, optional
        Set of all known dimensions, used to expand and validate any used
        in lookup keys.

    Returns
    -------
    contents : `dict` of `LookupKey` to `str`
        A `dict` with keys constructed from the configuration keys and values
        being simple strings.  It is assumed the caller will convert the
        values to the required form.

    Notes
    -----
    The configuration is a mapping where the keys correspond to names
    that can refer to dataset type or storage class names, or can use a
    special syntax to refer to dimensions or dataId values.

    Dimensions are indicated by using dimension names separated by a ``+``.
    If a single dimension is specified this is also supported so long as
    a ``+`` is found.  Dimensions are normalized before use such that if
    ``physical_filter+visit`` is defined, then an implicit ``instrument``
    will automatically be added.

    DataID overrides can be specified using the form: ``field<value>`` to
    indicate a subhierarchy.  All keys within that new hierarchy will take
    precedence over equivalent values in the root hierarchy.

    Currently only a single dataId field can be specified for a key.
    For example with a config such as:

    .. code::

       something:
         calexp: value1
         instrument<HSC>:
           calexp: value2

    Requesting the match for ``calexp`` would return ``value1`` unless
    a `DatasetRef` is used with a dataId containing the key ``instrument``
    and value ``HSC``.

    The values of the mapping are stored as strings.
    """
    contents = {}
    for name, value in config.items():
        if isinstance(value, Mapping):
            # indicates a dataId component -- check the format
            kv = DATAID_RE.match(name)
            if kv:
                dataIdKey = kv.group(1)
                dataIdValue = kv.group(2)
                for subKey, subStr in value.items():
                    lookup = LookupKey(name=subKey, dataId={dataIdKey: dataIdValue}, universe=universe)
                    contents[lookup] = subStr
            else:
                raise RuntimeError(f"Hierarchical key '{name}' not in form 'key<value>'")
        else:
            lookup = LookupKey(name=name, universe=universe)
            contents[lookup] = value

    return contents


def processLookupConfigList(config, *, universe=None):
    """Process sections of configuration relating to lookups by dataset type
    name, storage class name, dimensions, or values of dimensions.

    Parameters
    ----------
    config : `list` of `str` or `dict`
        Contents a configuration listing keys that can be
        dataset type names, storage class names, dimensions
        or dataId components.  DataId components are represented as entries
        in the `list` of `dicts` with a single key with a value of a `list`
        of new keys.
    universe : `DimensionUniverse`, optional
        Set of all known dimensions, used to expand and validate any used
        in lookup keys.

    Returns
    -------
    lookups : `set` of `LookupKey`
        All the entries in the input list converted to `LookupKey` and
        returned in a `set`.

    Notes
    -----
    Keys are parsed as described in `processLookupConfigs`.
    """
    contents = set()

    for name in config:
        if isinstance(name, Mapping):
            if len(name) != 1:
                raise RuntimeError(f"Config dict entry {name} has more than key present")
            for dataIdLookUp, subKeys in name.items():
                kv = DATAID_RE.match(dataIdLookUp)
                if kv:
                    dataIdKey = kv.group(1)
                    dataIdValue = kv.group(2)
                    for subKey in subKeys:
                        lookup = LookupKey(name=subKey, dataId={dataIdKey: dataIdValue}, universe=universe)
                        contents.add(lookup)
                else:
                    raise RuntimeError(f"Hierarchical key '{name}' not in form 'key<value>'")
        else:
            contents.add(LookupKey(name=name, universe=universe))

    return contents
