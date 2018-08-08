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

from collections.abc import MutableMapping

from .utils import doImport

__all__ = ("DatabaseDict",)


class DatabaseDict(MutableMapping):
    """An abstract base class for dict-like objects with a specific key type
    and namedtuple values, backed by a database.

    DatabaseDict subclasses must implement the abstract ``__getitem__``,
    ``__setitem__``, ``__delitem__`, ``__iter__``, and ``__len__`` abstract
    methods defined by `~collections.abc.MutableMapping`.

    They must also provide a constructor that takes the same arguments as that
    of `DatabaseDict` itself, *unless* they are constructed solely by
    `Registry.makeDatabaseDict` (in which case any constructor arguments are
    permitted).

    Parameters
    ----------
    config : `Config`
        Configuration used to identify and construct a subclass.
    types : `dict`
        A dictionary mapping `str` field names to type objects, containing
        all fields to be held in the database.
    key : `str`
        The name of the field to be used as the dictionary key.  Must not be
        present in ``value._fields``.
    value : `type`
        The type used for the dictionary's values, typically a `namedtuple`.
        Must have a ``_fields`` class attribute that is a tuple of field names
        (i.e. as defined by `namedtuple`); these field names must also appear
        in the ``types`` arg, and a `_make` attribute to construct it from a
        sequence of values (again, as defined by `namedtuple`).
    """

    @staticmethod
    def fromConfig(config, types, key, value, registry=None):
        """Create a `DatabaseDict` subclass instance from `config`.

        If ``config`` contains a class ``cls`` key, this will be assumed to
        be the fully-qualified name of a DatabaseDict subclass to construct.
        If not, ``registry.makeDatabaseDict`` will be called instead, and
        ``config`` must contain a ``table`` key with the name of the table
        to use.

        Parameters
        ----------
        config : `Config`
            Configuration used to identify and construct a subclass.
        types : `dict`
            A dictionary mapping `str` field names to type objects, containing
            all fields to be held in the database.
        key : `str`
            The name of the field to be used as the dictionary key.  Must not
            be present in ``value._fields``.
        value : `type`
            The type used for the dictionary's values, typically a
            `namedtuple`. Must have a ``_fields`` class attribute that is a
            tuple of field names (i.e. as defined by `namedtuple`); these
            field names must also appear in the ``types`` arg, and a `_make`
            attribute to construct it from a sequence of values (again, as
            defined by `namedtuple`).
        registry : `Registry`
            A registry instance from which a `DatabaseDict` subclass can be
            obtained.  Ignored if ``config["cls"]`` exists; may be None if
            it does.

        Returns
        -------
        dictionary : `DatabaseDict` (subclass)
            A new `DatabaseDict` subclass instance.
        """
        if "cls" in config:
            cls = doImport(config["cls"])
            return cls(config=config, types=types, key=key, value=value)
        else:
            table = config["table"]
            if registry is None:
                raise ValueError("Either config['cls'] or registry must be provided.")
            return registry.makeDatabaseDict(table, types=types, key=key, value=value)

    def __init__(self, config, types, key, value):
        # This constructor is currently defined just to clearly document the
        # interface subclasses should conform to.
        pass
