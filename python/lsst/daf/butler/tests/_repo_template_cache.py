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

from __future__ import annotations

__all__ = [
    "TemplateCacheStats",
    "clear_repo_template_cache",
    "make_repo_for_test",
    "template_cache_stats",
]

import atexit
import dataclasses
import hashlib
import json
import os
import shutil
import tempfile
import urllib.parse
from typing import Any

import pydantic

from lsst.resources import ResourcePath, ResourcePathExpression
from lsst.resources.file import FileResourcePath

from .. import Butler, Config
from ..dimensions import DimensionConfig
from ..repo_relocation import replaceRoot

# The environment variable that alters which default configuration files are
# found, and therefore what a given input configuration expands to.
_CONFIG_PATH_ENV = "DAF_BUTLER_CONFIG_PATH"


class TemplateCacheStats(pydantic.BaseModel):
    """Counts of cache activity, for tests and diagnostics."""

    served: int = 0
    """Requests handled by this helper."""

    bypassed: int = 0
    """Requests that went straight to `lsst.daf.butler.Butler.makeRepo`."""

    config_templates: int = 0
    """Configurations written from scratch and retained for reuse."""

    reused_config: int = 0
    """Requests whose ``butler.yaml`` was copied from an earlier identical
    one."""

    templates: int = 0
    """Databases actually built."""

    reused_database: int = 0
    """Requests whose database was copied from an earlier identical one."""


@dataclasses.dataclass(frozen=True)
class _ConfigTemplate:
    """A retained repository configuration, ready to be copied."""

    path: str
    """Path to a pristine copy of ``butler.yaml``."""

    config: Config
    """The configuration that repository creation returned.

    This is retained alongside the file rather than re-read from it because
    the two differ: the obscore manager configuration is deliberately stripped
    before writing, since it is stored in the registry instead, but registry
    creation still needs it.
    """


# Whole-configuration hash -> the configuration it produces.
_configs: dict[str, _ConfigTemplate] = {}
# Registry-and-dimensions hash -> path to a pristine copy of the database.
_databases: dict[str, str] = {}
_tmpdirs: list[str] = []
_stats = TemplateCacheStats()


def template_cache_stats() -> TemplateCacheStats:
    """Return counts of cache activity, for tests and diagnostics.

    Returns
    -------
    stats : `TemplateCacheStats`
        A snapshot of the counters. Later activity does not change it.
    """
    return _stats.model_copy()


def clear_repo_template_cache() -> None:
    """Discard all cached templates and reset the statistics."""
    global _stats

    for directory in _tmpdirs:
        shutil.rmtree(directory, ignore_errors=True)
    _tmpdirs.clear()
    _configs.clear()
    _databases.clear()
    _stats = TemplateCacheStats()


atexit.register(clear_repo_template_cache)


def _is_cacheable_registry(config: Config | None) -> bool:
    """Return whether this repository's registry can be served from a copy.

    Parameters
    ----------
    config : `lsst.daf.butler.Config` or `None`
        Repository configuration, or `None` to accept the defaults.

    Returns
    -------
    cacheable : `bool`
        `True` if the registry lives in a SQLite file inside the repository,
        which is the only case a directory copy can reproduce.

    Notes
    -----
    A client/server database such as PostgreSQL keeps its contents outside the
    repository directory, so copying the directory does not copy the registry.
    Such repositories also carry a per-repository ``namespace``, which makes
    every configuration unique and every cache lookup a miss. Caching them
    would build a template that is used exactly once and then retained, which
    is strictly more work than creating the repository directly.
    """
    if config is None:
        # The default registry is SQLite inside the repository.
        return True
    db = config.get(("registry", "db"))
    if db is None:
        return True
    return str(db).startswith("sqlite")


def make_repo_for_test(
    root: ResourcePathExpression,
    config: Config | str | None = None,
    dimensionConfig: Config | str | None = None,
    standalone: bool = False,
    searchPaths: list[str] | None = None,
    forceConfigRoot: bool = True,
    outfile: ResourcePathExpression | None = None,
    overwrite: bool = False,
) -> Config:
    """Create a test repository, reusing a cached template when possible.

    The parameters and return value match
    `lsst.daf.butler.Butler.makeRepo`. The first request for a given
    configuration builds a real repository; later requests copy it, which is
    substantially cheaper.

    Parameters
    ----------
    root : `lsst.resources.ResourcePathExpression`
        Path to the root location of the new repository.
    config : `lsst.daf.butler.Config` or `str`, optional
        Configuration to write to the repository.
    dimensionConfig : `lsst.daf.butler.Config` or `str`, optional
        Configuration for dimensions.
    standalone : `bool`, optional
        If `True`, write all expanded defaults. Bypasses the cache.
    searchPaths : `list` [`str`], optional
        Directory paths to search when calculating the full configuration.
        Bypasses the cache.
    forceConfigRoot : `bool`, optional
        If `False`, values present in ``config`` that would normally be reset
        are not overridden.
    outfile : `lsst.resources.ResourcePathExpression`, optional
        Path at which to write the config. Bypasses the cache.
    overwrite : `bool`, optional
        If `True`, allow an existing config to be overwritten. Bypasses the
        cache.

    Returns
    -------
    config : `lsst.daf.butler.Config`
        The configuration of the new repository.

    Raises
    ------
    FileExistsError
        Raised if the repository already has a configuration and ``overwrite``
        is `False`.

    Notes
    -----
    This helper is for test code only. Production code, and any test that
    asserts on the behavior of repository creation itself rather than on its
    result, must call `lsst.daf.butler.Butler.makeRepo` directly.
    """
    if isinstance(config, str):
        # Read the file now rather than treating its name as the identity of
        # its contents: tests rewrite temporary configuration files in place,
        # and a cache keyed on the pathname would serve the stale version.
        # This is the same conversion repository creation performs.
        config = Config(config)

    # RemoteTestResourcePath subclasses FileResourcePath and reports
    # isLocal=False while remaining backed by a local path, so isLocal is the
    # wrong question to ask here.
    copyable = isinstance(ResourcePath(root, forceDirectory=True), FileResourcePath)

    usable = (
        copyable
        and _is_cacheable_registry(config)
        and outfile is None
        and not standalone
        and not overwrite
        and not searchPaths
    )

    if not usable:
        _stats.bypassed += 1
        return Butler.makeRepo(
            root,
            config=config,
            dimensionConfig=dimensionConfig,
            standalone=standalone,
            searchPaths=searchPaths,
            forceConfigRoot=forceConfigRoot,
            outfile=outfile,
            overwrite=overwrite,
        )

    # Phase one: the repository directory and its butler.yaml. This depends on
    # the whole configuration, so it is cached on a hash of all of it. The
    # written file is root-independent because paths are stored against the
    # repository root tag, so a copy is valid anywhere.
    written, root_uri = _make_butler_config(root, config, forceConfigRoot)

    # Phase two: the database. Only the registry and dimension configurations
    # affect its contents, so it is cached on those alone and copied into
    # place.
    db_key = _database_key(written, dimensionConfig)
    db_path = _sqlite_path(written, root_uri)
    if db_key is None or db_path is None:
        _stats.served += 1
        Butler._make_repo_registry(written, dimensionConfig=dimensionConfig, root_uri=root_uri)
        return written

    cached_db = _databases.get(db_key)
    if cached_db is None:
        _stats.templates += 1
        Butler._make_repo_registry(written, dimensionConfig=dimensionConfig, root_uri=root_uri)
        if not os.path.exists(db_path):
            # Registry creation put the database somewhere other than where
            # this helper expects it, so there is nothing safe to retain. The
            # repository itself is complete, so report it as served and leave
            # the database uncached rather than failing.
            _stats.served += 1
            return written
        holder = tempfile.mkdtemp(prefix="butler-registry-template-")
        _tmpdirs.append(holder)
        cached_db = os.path.join(holder, os.path.basename(db_path))
        shutil.copyfile(db_path, cached_db)
        _databases[db_key] = cached_db
    else:
        _stats.reused_database += 1
        shutil.copyfile(cached_db, db_path)

    _stats.served += 1
    return written


def _make_butler_config(
    root: ResourcePathExpression,
    config: Config | None,
    forceConfigRoot: bool,
) -> tuple[Config, ResourcePath]:
    """Write the repository's ``butler.yaml``, reusing an identical one.

    Parameters
    ----------
    root : `lsst.resources.ResourcePathExpression`
        Path to the root location of the new repository.
    config : `lsst.daf.butler.Config` or `None`
        Repository configuration.
    forceConfigRoot : `bool`
        Whether root-dependent options are overridden.

    Returns
    -------
    written : `lsst.daf.butler.Config`
        The configuration written to the repository.
    root_uri : `lsst.resources.ResourcePath`
        The root of the new repository.

    Raises
    ------
    FileExistsError
        Raised if the repository already has a configuration.
    """
    key = _config_key(config, forceConfigRoot)
    cached = _configs.get(key) if key is not None else None
    if cached is None:
        written, root_uri = Butler._make_repo_butler_config(
            root, config=config, forceConfigRoot=forceConfigRoot
        )
        if key is not None:
            _stats.config_templates += 1
            holder = tempfile.mkdtemp(prefix="butler-config-template-")
            _tmpdirs.append(holder)
            path = os.path.join(holder, "butler.yaml")
            shutil.copyfile(os.path.join(root_uri.ospath, "butler.yaml"), path)
            # Retain a copy so that a caller mutating the returned
            # configuration cannot reach the template.
            _configs[key] = _ConfigTemplate(path=path, config=written.copy())
        return written, root_uri

    _stats.reused_config += 1
    root_uri = ResourcePath(root, forceDirectory=True)
    root_uri.mkdir()
    destination = ResourcePath(os.path.join(root_uri.ospath, "butler.yaml"), forceDirectory=False)
    # Exclusive creation reproduces the FileExistsError that writing the
    # configuration would raise, since only overwrite=False reaches here.
    with open(cached.path, "rb") as source, open(destination.ospath, "xb") as target:
        shutil.copyfileobj(source, target)
    written = cached.config.copy()
    written.configFile = destination
    return written, root_uri


def _config_key(config: Config | None, forceConfigRoot: bool) -> str | None:
    """Return a key covering everything that affects ``butler.yaml``.

    Parameters
    ----------
    config : `lsst.daf.butler.Config` or `None`
        Repository configuration.
    forceConfigRoot : `bool`
        Whether root-dependent options are overridden.

    Returns
    -------
    key : `str` or `None`
        A hash of the inputs, or `None` if they cannot be rendered
        deterministically.

    Notes
    -----
    Two inputs beyond the configuration itself change what is written, so both
    take part in the key. ``forceConfigRoot`` decides whether root-dependent
    values in the supplied configuration survive into the file, and
    ``DAF_BUTLER_CONFIG_PATH`` decides which default configuration files the
    supplied one is expanded against. Only a handful of tests vary either, so
    including them costs a cache miss in those tests and nothing elsewhere.
    """
    try:
        rendered = json.dumps(
            [
                config.toDict() if config is not None else None,
                forceConfigRoot,
                os.environ.get(_CONFIG_PATH_ENV),
            ],
            sort_keys=True,
            default=str,
        )
    except (TypeError, ValueError):
        return None
    return hashlib.sha256(rendered.encode()).hexdigest()


def _database_key(written: Config, dimensionConfig: Config | str | None) -> str | None:
    """Return a key covering everything that affects the database contents.

    Parameters
    ----------
    written : `lsst.daf.butler.Config`
        The repository configuration that was written to ``butler.yaml``.
    dimensionConfig : `lsst.daf.butler.Config` or `str` or `None`
        Dimension universe configuration.

    Returns
    -------
    key : `str` or `None`
        A hash of the registry and dimension configurations, or `None` if
        they cannot be rendered deterministically.

    Notes
    -----
    Datastore configuration, storage classes and other sections do not reach
    the database, so they are deliberately excluded. The ``db`` entry is also
    excluded because it only names the file's location, which differs between
    repositories that are otherwise identical.
    """
    try:
        registry = dict(written["registry"].toDict())
        registry.pop("db", None)
        rendered = json.dumps(
            [registry, _dimension_key_material(dimensionConfig), os.environ.get(_CONFIG_PATH_ENV)],
            sort_keys=True,
            default=str,
        )
    except Exception:
        # Any failure here means the inputs cannot be identified cheaply, and
        # the caller falls back to creating the database directly. Letting the
        # exception out would report it from key derivation rather than from
        # the registry creation that will raise it again in context.
        return None
    return hashlib.sha256(rendered.encode()).hexdigest()


def _dimension_key_material(dimensionConfig: Config | str | None) -> Any:
    """Return the part of a cache key that identifies the dimension universe.

    Parameters
    ----------
    dimensionConfig : `lsst.daf.butler.Config` or `str` or `None`
        Dimension universe configuration, as passed to repository creation.

    Returns
    -------
    material : `object`
        A JSON-serializable description of the configuration.

    Notes
    -----
    `None` contributes nothing, because the defaults it selects are determined
    by the configuration search path, which the key covers separately.
    """
    if dimensionConfig is None:
        return None
    if isinstance(dimensionConfig, Config):
        return dimensionConfig.toDict()
    # A pathname says nothing about the file's contents, and a relative name is
    # resolved against the configuration search path, so expand it exactly as
    # registry creation will.
    return DimensionConfig(dimensionConfig).toDict()


def _sqlite_path(written: Config, root_uri: ResourcePath) -> str | None:
    """Return the local path of the repository's SQLite file, if it has one.

    Parameters
    ----------
    written : `lsst.daf.butler.Config`
        The repository configuration that was written to ``butler.yaml``.
    root_uri : `lsst.resources.ResourcePath`
        Root of the repository, substituted for the repository root tag.

    Returns
    -------
    path : `str` or `None`
        Path to the SQLite file, or `None` if the registry is not a SQLite
        file inside the repository.

    Notes
    -----
    The location is derived the way the registry derives it, in two steps:
    `lsst.daf.butler.repo_relocation.replaceRoot` substitutes the repository
    root, then the result is parsed as a URI, which is what
    ``SqliteDatabase.makeEngine`` does to find the file it opens.

    Reconstructing the path instead of following those two steps gives the
    wrong answer whenever the root holds a URI metacharacter, because
    ``replaceRoot`` substitutes a root whose ``#`` fragment has already been
    dropped and the parse then discards everything from a ``?`` onwards. Such
    a root is mangled by repository creation itself, and this helper has to
    land on the same mangled path rather than on the one the caller asked for.
    """
    db = written.get(("registry", "db"))
    if db is None:
        return None
    resolved = replaceRoot(str(db), root_uri)
    parsed = urllib.parse.urlparse(resolved)
    if parsed.scheme != "sqlite" or not parsed.path.startswith("/"):
        return None
    location = parsed.path[1:]
    if not location or location == ":memory:":
        return None
    return location
