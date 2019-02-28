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

__all__ = ("LookupKey", "processLookupConfigs", "normalizeLookupKeys")

import logging
import re
from collections.abc import Mapping
from .dimensions import DimensionNameSet, DimensionGraph

log = logging.getLogger(__name__)


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
    """

    def __init__(self, name=None, dimensions=None, dataId=None):
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
                self._dimensions = DimensionNameSet(dimensions)
            else:
                self._name = name
        else:
            self._dimensions = dimensions

        # The dataId is converted to a frozenset of key/value
        # tuples.
        if dataId is not None:
            self._dataId = frozenset((k, v) for k, v in dataId.items())
        else:
            self._dataId = None

    def __str__(self):
        return "({}, {})".format(self._name if self._name else self._dimensions,
                                 ",".join(str(t) for t in self._dataId) if self._dataId else "")

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
        """Set of key/value tuples that are important for dataId lookup.
        (`frozenset`)"""
        return self._dataId

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

        # To copy the dataId we need to convert it back to a dict when
        # copying
        if dataId is None and self._dataId is not None:
            dataId = {k: v for k, v in self._dataId}

        return self.__class__(name=name, dimensions=dimensions, dataId=dataId)


def normalizeLookupKeys(toUpdate, universe):
    """Normalize dimensions used in keys of supplied dict.

    Parameters
    ----------
    toUpdate : `dict` with keys of `LookupKey`
        Dictionary to update.  The values are reassigned to normalized
        versions of the keys.  Keys are ignored that are not `LookupKey`.
    universe : `DimensionUniverse`
        The set of all known dimensions. If `None`, returns without
        action.

    Notes
    -----
    Goes through all keys, and for keys that include
    dimensions, rewrites those keys to use a verified set of
    dimensions.

    Raises
    ------
    ValueError
        Raised if a key exists where a dimension is not part of
        the ``universe``.
    """
    if universe is None:
        return

    # Get the keys because we are going to change them
    allKeys = list(toUpdate.keys())

    for k in allKeys:
        if not isinstance(k, LookupKey):
            continue
        if k.dimensions is not None and not isinstance(k.dimensions, DimensionGraph):
            newDimensions = universe.extract(k.dimensions)
            newKey = k.clone(dimensions=newDimensions)
            toUpdate[newKey] = toUpdate[k]
            del toUpdate[k]


def processLookupConfigs(config):
    """Process sections of configuration relating to lookups by dataset type
    name, storage class name, dataId components or dimensions.

    Parameters
    ----------
    config : `Config`
        A `Config` representing a configuration mapping keys to values where
        the keys can be dataset type names, storage class names, dimensions
        or dataId components.

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
    ``PhysicalFilter+Visit`` is defined, then an implicit ``Instrument``
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
            kv = re.match(r"([a-z_]+)<(.*)>$", name)
            if kv:
                dataIdKey = kv.group(1)
                dataIdValue = kv.group(2)
                for subKey, subStr in value.items():
                    lookup = LookupKey(name=subKey, dataId={dataIdKey: dataIdValue})
                    contents[lookup] = subStr
            else:
                log.warning("Hierarchical key '%s' not in form 'key<value>', ignoring", name)
        else:
            lookup = LookupKey(name=name)
            contents[lookup] = value

    return contents
