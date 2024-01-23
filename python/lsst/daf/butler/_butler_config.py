# This file is part of daf_butler.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (http://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This software is dual licensed under the GNU General Public License and also
# under a 3-clause BSD license. Recipients may choose which of these licenses
# to use; please see the files gpl-3.0.txt and/or bsd_license.txt,
# respectively.  If you choose the GPL option then the following text applies
# (but note that there is still no warranty even if you opt for BSD instead):
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

"""Configuration classes specific to the Butler.
"""
from __future__ import annotations

__all__ = ("ButlerConfig",)

import contextlib
import copy
import os
from collections.abc import Sequence
from enum import Enum

from lsst.resources import ResourcePath, ResourcePathExpression

from ._butler_repo_index import ButlerRepoIndex
from ._config import Config
from ._storage_class import StorageClassConfig
from .datastore import DatastoreConfig
from .registry import RegistryConfig
from .transfers import RepoTransferFormatConfig

CONFIG_COMPONENT_CLASSES = (RegistryConfig, StorageClassConfig, DatastoreConfig, RepoTransferFormatConfig)

ButlerType = Enum("ButlerType", ["DIRECT", "REMOTE"])


class ButlerConfig(Config):
    """Contains the configuration for a `Butler`.

    The configuration is read and merged with default configurations for
    the particular classes. The defaults are read according to the rules
    outlined in `ConfigSubset`. Each component of the configuration associated
    with a configuration class reads its own defaults.

    Parameters
    ----------
    other : `str`, `Config`, `ButlerConfig`, optional
        Path to butler configuration YAML file or a directory containing a
        "butler.yaml" file. If `None` the butler will
        be configured based entirely on defaults read from the environment
        or from ``searchPaths``.
        No defaults will be read if a `ButlerConfig` is supplied directly.
    searchPaths : `list` or `tuple`, optional
        Explicit additional paths to search for defaults. They should
        be supplied in priority order. These paths have higher priority
        than those read from the environment in
        `ConfigSubset.defaultSearchPaths()`.  They are only read if ``other``
        refers to a configuration file or directory.
    without_datastore : `bool`, optional
        If `True` remove the datastore configuration.
    """

    def __init__(
        self,
        other: ResourcePathExpression | Config | None = None,
        searchPaths: Sequence[ResourcePathExpression] | None = None,
        without_datastore: bool = False,
    ):
        self.configDir: ResourcePath | None = None

        # If this is already a ButlerConfig we assume that defaults
        # have already been loaded.
        if other is not None and isinstance(other, ButlerConfig):
            super().__init__(other)
            # Ensure that the configuration directory propagates
            self.configDir = copy.copy(other.configDir)
            return

        # If a string is given it *could* be an alias that should be
        # expanded by the repository index system.
        original_other = other
        resolved_alias = False
        if isinstance(other, str):
            with contextlib.suppress(Exception):
                # Force back to a string because the resolved URI
                # might not refer explicitly to a directory and we have
                # check below to guess that.
                other = str(ButlerRepoIndex.get_repo_uri(other, False))
            if other != original_other:
                resolved_alias = True

        # Include ResourcePath here in case it refers to a directory.
        # Creating a ResourcePath from a ResourcePath is a no-op.
        if isinstance(other, str | os.PathLike | ResourcePath):
            # This will only allow supported schemes
            uri = ResourcePath(other)

            # We allow the butler configuration file to be left off the
            # URI supplied by the user. If a directory-like URI is given
            # we add the default configuration name.

            # It's easy to miss a trailing / for remote URIs so try to guess
            # we have been given a directory-like URI if there is no
            # file extension. Local URIs do not need any guess work.
            if not uri.isLocal and not uri.getExtension():
                uri = ResourcePath(other, forceDirectory=True)

            if uri.isdir():
                # Could also be butler.json (for example in the butler
                # server) but checking for existence will slow things
                # down given that this might involve two checks and then
                # the config read below would still do the read.
                other = uri.join("butler.yaml", forceDirectory=False)

        # Create an empty config for us to populate
        super().__init__()

        # Read the supplied config so that we can work out which other
        # defaults to use.
        try:
            butlerConfig = Config(other)
        except FileNotFoundError as e:
            # No reason to talk about aliases unless we were given a
            # string and the alias was not resolved.
            if isinstance(original_other, str):
                if not resolved_alias:
                    # No alias was resolved. List known aliases if we have
                    # them or else explain a reason why aliasing might not
                    # have happened.
                    if known := ButlerRepoIndex.get_known_repos():
                        aliases = f"(given {original_other!r} and known aliases: {', '.join(known)})"
                    else:
                        failure_reason = ButlerRepoIndex.get_failure_reason()
                        if failure_reason:
                            failure_reason = f": {failure_reason}"
                        aliases = f"(given {original_other!r} and no known aliases{failure_reason})"
                else:
                    aliases = f"(resolved from alias {original_other!r})"
                errmsg = f"{e} {aliases}"
            else:
                errmsg = str(e)
            raise FileNotFoundError(errmsg) from e

        configFile = butlerConfig.configFile
        if configFile is not None:
            uri = ResourcePath(configFile)
            self.configFile = uri
            self.configDir = uri.dirname()

        # A Butler config contains defaults defined by each of the component
        # configuration classes. We ask each of them to apply defaults to
        # the values we have been supplied by the user.
        for configClass in CONFIG_COMPONENT_CLASSES:
            assert configClass.component is not None, "Config class component cannot be None"

            if without_datastore and configClass is DatastoreConfig:
                if configClass.component in butlerConfig:
                    del butlerConfig[configClass.component]
                continue

            # Only send the parent config if the child
            # config component is present (otherwise it assumes that the
            # keys from other components are part of the child)
            localOverrides = None
            if configClass.component in butlerConfig:
                localOverrides = butlerConfig
            config = configClass(localOverrides, searchPaths=searchPaths)
            # Re-attach it using the global namespace
            self.update({configClass.component: config})
            # Remove the key from the butlerConfig since we have already
            # merged that information.
            if configClass.component in butlerConfig:
                del butlerConfig[configClass.component]

        # Now that we have all the defaults we can merge the externally
        # provided config into the defaults.
        # Not needed if there is never information in a butler config file
        # not present in component configurations
        self.update(butlerConfig)

    def get_butler_type(self) -> ButlerType:
        # Configuration optionally includes a class name specifying which
        # implementation to use, DirectButler or RemoteButler.
        butler_class_name = self.get("cls")
        if butler_class_name is None:
            # There are many existing DirectButler configurations that are
            # missing the ``cls`` property.
            return ButlerType.DIRECT
        elif butler_class_name == "lsst.daf.butler.direct_butler.DirectButler":
            return ButlerType.DIRECT
        elif butler_class_name == "lsst.daf.butler.remote_butler.RemoteButler":
            return ButlerType.REMOTE
        else:
            raise ValueError(
                f"Butler configuration requests to load unknown Butler class {butler_class_name}"
            )
