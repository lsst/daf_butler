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

__all__ = ["clear_repo_template_cache", "make_repo_for_test", "template_cache_stats"]

import atexit
import hashlib
import json
import os
import shutil
import tempfile
from collections import Counter

from lsst.resources import ResourcePath, ResourcePathExpression
from lsst.resources.file import FileResourcePath

from .. import Butler, Config
from ..repo_relocation import BUTLER_ROOT_TAG

# Whole-configuration hash -> path to a pristine copy of butler.yaml.
_configs: dict[str, str] = {}
# Registry-and-dimensions hash -> path to a pristine copy of the database.
_databases: dict[str, str] = {}
_tmpdirs: list[str] = []
_stats: Counter[str] = Counter()


def template_cache_stats() -> dict[str, int]:
    """Return counts of cache activity, for tests and diagnostics.

    Returns
    -------
    stats : `dict` [`str`, `int`]
        Keys are ``templates`` (databases actually built), ``served``
        (requests handled by this helper), ``bypassed`` (requests that
        went straight to `lsst.daf.butler.Butler.makeRepo`), and
        ``reused_database`` (requests whose database was copied from an
        earlier identical one).
    """
    return {
        key: _stats[key]
        for key in (
            "served",
            "bypassed",
            "config_templates",
            "reused_config",
            "templates",
            "reused_database",
        )
    }


def clear_repo_template_cache() -> None:
    """Discard all cached templates and reset the statistics."""
    for directory in _tmpdirs:
        shutil.rmtree(directory, ignore_errors=True)
    _tmpdirs.clear()
    _configs.clear()
    _databases.clear()
    _stats.clear()


atexit.register(clear_repo_template_cache)


def _is_cacheable_registry(config: Config | str | None) -> bool:
    """Return whether this repository's registry can be served from a copy.

    Parameters
    ----------
    config : `lsst.daf.butler.Config` or `str` or `None`
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
    if not isinstance(config, Config):
        config = Config(config)
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
        The configuration of the new repository, read from ``root``.

    Notes
    -----
    This helper is for test code only. Production code, and any test that
    asserts on the behavior of repository creation itself rather than on its
    result, must call `lsst.daf.butler.Butler.makeRepo` directly.
    """
    resource = ResourcePath(root, forceDirectory=True)
    # RemoteTestResourcePath subclasses FileResourcePath and reports
    # isLocal=False while remaining backed by a local path, so isLocal is the
    # wrong question to ask here.
    copyable = isinstance(resource, FileResourcePath)

    usable = (
        copyable
        and _is_cacheable_registry(config)
        and outfile is None
        and not standalone
        and not overwrite
        and not searchPaths
    )

    if not usable:
        _stats["bypassed"] += 1
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
    written, root_uri = _make_butler_config(root, config, forceConfigRoot, resource)

    # Phase two: the database. Only the registry and dimension configurations
    # affect its contents, so it is cached on those alone and copied into
    # place.
    db_key = _database_key(written, dimensionConfig)
    db_path = _sqlite_path(written, root_uri)
    if db_key is None or db_path is None:
        _stats["served"] += 1
        Butler._make_repo_registry(written, dimensionConfig=dimensionConfig, root_uri=root_uri)
        return written

    cached = _databases.get(db_key)
    if cached is None:
        _stats["templates"] += 1
        Butler._make_repo_registry(written, dimensionConfig=dimensionConfig, root_uri=root_uri)
        holder = tempfile.mkdtemp(prefix="butler-registry-template-")
        _tmpdirs.append(holder)
        cached = os.path.join(holder, "gen3.sqlite3")
        shutil.copyfile(db_path, cached)
        _databases[db_key] = cached
    else:
        _stats["reused_database"] += 1
        shutil.copyfile(cached, db_path)

    _stats["served"] += 1
    return written


def _make_butler_config(
    root: ResourcePathExpression,
    config: Config | str | None,
    forceConfigRoot: bool,
    resource: ResourcePath,
) -> tuple[Config, ResourcePath]:
    """Write the repository's ``butler.yaml``, reusing an identical one.

    Parameters
    ----------
    root : `lsst.resources.ResourcePathExpression`
        Path to the root location of the new repository.
    config : `lsst.daf.butler.Config` or `str` or `None`
        Repository configuration.
    forceConfigRoot : `bool`
        Whether root-dependent options are overridden.
    resource : `lsst.resources.ResourcePath`
        ``root`` resolved to a directory resource.

    Returns
    -------
    written : `lsst.daf.butler.Config`
        The configuration written to the repository.
    root_uri : `lsst.resources.ResourcePath`
        The root of the new repository.
    """
    key = _config_key(config, forceConfigRoot)
    cached = _configs.get(key) if key is not None else None
    if cached is None:
        written, root_uri = Butler._make_repo_butler_config(
            root, config=config, forceConfigRoot=forceConfigRoot
        )
        if key is not None:
            _stats["config_templates"] += 1
            holder = tempfile.mkdtemp(prefix="butler-config-template-")
            _tmpdirs.append(holder)
            path = os.path.join(holder, "butler.yaml")
            shutil.copyfile(os.path.join(root_uri.ospath, "butler.yaml"), path)
            _configs[key] = path
        return written, root_uri

    _stats["reused_config"] += 1
    root_uri = ResourcePath(root, forceDirectory=True)
    root_uri.mkdir()
    destination = os.path.join(root_uri.ospath, "butler.yaml")
    shutil.copyfile(cached, destination)
    return Config(destination), root_uri


def _config_key(config: Config | str | None, forceConfigRoot: bool) -> str | None:
    """Return a key covering everything that affects ``butler.yaml``.

    Parameters
    ----------
    config : `lsst.daf.butler.Config` or `str` or `None`
        Repository configuration.
    forceConfigRoot : `bool`
        Whether root-dependent options are overridden.

    Returns
    -------
    key : `str` or `None`
        A hash of the inputs, or `None` if they cannot be rendered
        deterministically.
    """
    try:
        rendered = json.dumps(
            [
                config.toDict() if isinstance(config, Config) else config,
                forceConfigRoot,
                os.environ.get("DAF_BUTLER_CONFIG_PATH"),
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
        dimensions = dimensionConfig.toDict() if isinstance(dimensionConfig, Config) else dimensionConfig
        rendered = json.dumps([registry, dimensions], sort_keys=True, default=str)
    except (AttributeError, KeyError, TypeError, ValueError):
        return None
    return hashlib.sha256(rendered.encode()).hexdigest()


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
    """
    db = written.get(("registry", "db"))
    if db is None or not str(db).startswith("sqlite:///"):
        return None
    location = str(db)[len("sqlite:///") :]
    if not location or location == ":memory:":
        return None
    location = location.replace(BUTLER_ROOT_TAG, root_uri.ospath.rstrip("/"))
    return location
