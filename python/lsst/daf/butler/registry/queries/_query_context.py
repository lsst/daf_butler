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

__all__ = ("QueryContext",)

from contextlib import AbstractContextManager
from typing import Any

from lsst.daf.relation import Engine, EngineError, Processor, Relation, iteration


class QueryContext(Processor, AbstractContextManager["QueryContext"]):
    """A context manager interface for query operations that require some
    connection-like state.

    Notes
    -----
    `QueryContext` implementations are usually paired with a `QueryBackend`
    implementation, with the division of responsibilities as follows:

    - `QueryContext` implements the `lsst.daf.relation.Processor` interface,
      and is hence responsible for executing multi-engine relation trees.

    - `QueryContext` manages all state whose lifetime is a single query or set
      of related queries (e.g. temporary tables) via its context manager
      interface.  Methods that do not involve this state should not require the
      context manager to have been entered.

    - `QueryContext` objects should be easily to construct by registry helper
      code that doesn't have access to the full `Registry` data structure
      itself, while `QueryBackend` instances can generally only be constructed
      by code that does see essentially the full registry (for example,
      `SqlQueryBackend` holds a `RegistryManagerInstances` struct, while
      `SqlQueryContext` can be constructed with just a `Database` and
      `ColumnTypeInfo`).

    - `QueryBackend.context` is a factory for the associated `QueryContext`
      type.

    - `QueryBackend` methods that return relations accept the `QueryContext`
      returned by its `~QueryBackend.context` method in case those methods
      require state that should be cleaned up after the query is complete.
    """

    def __init__(self) -> None:
        self.iteration_engine = iteration.Engine()

    iteration_engine: iteration.Engine
    """The relation engine that all relations must ultimately be transferred
    to in order to be executed by this context.
    """

    @property
    def preferred_engine(self) -> Engine:
        """Return the relation engine that this context prefers to execute
        operations in (`lsst.daf.relation.Engine`).
        """
        return self.iteration_engine

    def fetch_iterable(self, relation: Relation) -> iteration.RowIterable:
        """Execute the given relation and return its rows as an iterable of
        mappings.

        Parameters
        ----------
        relation : `Relation`
            Relation representing the query to execute.

        Returns
        -------
        rows : `~lsst.daf.relation.iteration.RowIterable`
            An iterable over rows, with each row a mapping from `ColumnTag`
            to column value.

        Notes
        -----
        A transfer to `iteration_engine` will be added to the root (end) of the
        relation tree if the root is not already in the iteration engine.

        Any transfers from other engines or persistent materializations will be
        handled by delegating to `process_relation` before execution in the
        iteration engine.

        To ensure the result is a multi-pass Python collection in memory,
        ensure the given tree ends with a materialization operation in the
        iteration engine.
        """
        # This transfer does nothing if the relation is already in the
        # iteration engine.
        relation = relation.transferred_to(self.iteration_engine)
        relation = self.process(relation)
        return self.iteration_engine.execute(relation)

    def transfer(self, source: Relation, destination: Engine, materialize_as: str | None) -> Any:
        # Docstring inherited from lsst.daf.relation.Processor.
        raise NotImplementedError("No transfers expected by base QueryContext implementation.")

    def materialize(self, base: Relation, name: str) -> Any:
        # Docstring inherited from lsst.daf.relation.Processor.
        if base.engine == self.iteration_engine:
            return self.iteration_engine.execute(base).materialized()
        raise EngineError(f"Unexpected engine {base.engine} for base QueryContext implementation.")
