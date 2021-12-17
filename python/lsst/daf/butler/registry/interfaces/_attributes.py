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

__all__ = [
    "ButlerAttributeManager",
    "ButlerAttributeExistsError",
]

from abc import abstractmethod
from typing import TYPE_CHECKING, Iterable, Optional, Tuple

from ._versioning import VersionedExtension

if TYPE_CHECKING:
    from ._database import Database, StaticTablesContext


class ButlerAttributeExistsError(RuntimeError):
    """Exception raised when trying to update existing attribute without
    specifying ``force`` option.
    """


class ButlerAttributeManager(VersionedExtension):
    """An interface for managing butler attributes in a `Registry`.

    Attributes are represented in registry as a set of name-value pairs, both
    have string type. Any non-string data types (e.g. integers) need to be
    converted to/from strings on client side. Attribute names can be arbitrary
    strings, no particular structure is enforced by this interface. Attribute
    names are globally unique, to avoid potential collision clients should
    follow some common convention for attribute names, e.g. dot-separated
    components (``config.managers.opaque``).

    One of the critical pieces of information that will be stored as
    attribute is the version of database schema which needs to be known
    before registry can do any operations on database. For that reasons it is
    likely there will be only one implementation of this interface which uses
    database table with a stable schema.
    """

    @classmethod
    @abstractmethod
    def initialize(cls, db: Database, context: StaticTablesContext) -> ButlerAttributeManager:
        """Construct an instance of the manager.

        Parameters
        ----------
        db : `Database`
            Interface to the underlying database engine and namespace.
        context : `StaticTablesContext`
            Context object obtained from `Database.declareStaticTables`; used
            to declare any tables that should always be present in a layer
            implemented with this manager.

        Returns
        -------
        manager : `ButlerAttributeManager`
            An instance of `ButlerAttributeManager`.
        """
        raise NotImplementedError()

    @abstractmethod
    def get(self, name: str, default: Optional[str] = None) -> Optional[str]:
        """Retrieve value of a given attribute.

        Parameters
        ----------
        name : `str`
            Attribute name, arbitrary non-empty string.
        default : `str`, optional
            Default value returned when attribute does not exist, can be
            string or `None`.

        Returns
        -------
        value : `str`
            Attribute value, if attribute does not exist then ``default`` is
            returned.
        """
        raise NotImplementedError()

    @abstractmethod
    def set(self, name: str, value: str, *, force: bool = False) -> None:
        """Set value for a given attribute.

        Parameters
        ----------
        name : `str`
            Attribute name, arbitrary non-empty string.
        value : `str`
            New value for an attribute, an arbitrary string. Due to
            deficiencies of some database engines we are not allowing empty
            strings to be stored in the database, and ``value`` cannot be an
            empty string.
        force : `bool`, optional
            Controls handling of existing attributes. With default `False`
            value an exception is raised if attribute ``name`` already exists,
            if `True` is passed then value of the existing attribute will be
            updated.

        Raises
        ------
        ButlerAttributeExistsError
            Raised if attribute already exists but ``force`` option is false.
        ValueError
            Raised if name or value parameters are empty.
        """
        raise NotImplementedError()

    @abstractmethod
    def delete(self, name: str) -> bool:
        """Delete an attribute.

        Parameters
        ----------
        name : `str`
            Attribute name, arbitrary non-empty string.

        Returns
        -------
        existed : `bool`
            `True` is returned if attribute existed before it was deleted.
        """
        raise NotImplementedError()

    @abstractmethod
    def items(self) -> Iterable[Tuple[str, str]]:
        """Iterate over attributes and yield their names and values.

        Yields
        ------
        name : `str`
            Attribute name.
        value : `str`
            Corresponding attribute value.
        """
        raise NotImplementedError()

    @abstractmethod
    def empty(self) -> bool:
        """Check whether attributes set is empty.

        Returns
        -------
        empty : `bool`
            True if there are no any attributes defined.
        """
        raise NotImplementedError()
