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

_CONFIG_PATH_ENV = "DAF_BUTLER_CONFIG_PATH"

_templates: dict[str, tuple[str, Config]] = {}
_tmpdirs: list[str] = []
_stats: Counter[str] = Counter()


def template_cache_stats() -> dict[str, int]:
    """Return counts of cache activity, for tests and diagnostics.

    Returns
    -------
    stats : `dict` [`str`, `int`]
        Keys are ``templates`` (repositories actually built), ``served``
        (requests satisfied by copying a template), and ``bypassed``
        (requests that went straight to `lsst.daf.butler.Butler.makeRepo`).
    """
    return {key: _stats[key] for key in ("templates", "served", "bypassed")}


def clear_repo_template_cache() -> None:
    """Discard all cached templates and reset the statistics."""
    for directory in _tmpdirs:
        shutil.rmtree(directory, ignore_errors=True)
    _tmpdirs.clear()
    _templates.clear()
    _stats.clear()


atexit.register(clear_repo_template_cache)


def _cache_key(
    config: Config | str | None,
    dimensionConfig: Config | str | None,
    forceConfigRoot: bool,
) -> str | None:
    """Return a stable key for this configuration, or `None` if unkeyable.

    Parameters
    ----------
    config : `lsst.daf.butler.Config` or `str` or `None`
        Repository configuration.
    dimensionConfig : `lsst.daf.butler.Config` or `str` or `None`
        Dimension universe configuration.
    forceConfigRoot : `bool`
        Whether root-dependent options are overridden.

    Returns
    -------
    key : `str` or `None`
        A hash of everything that affects the created repository, or `None`
        if the inputs cannot be rendered deterministically.
    """
    try:
        rendered = json.dumps(
            [
                config.toDict() if isinstance(config, Config) else config,
                dimensionConfig.toDict() if isinstance(dimensionConfig, Config) else dimensionConfig,
                forceConfigRoot,
                os.environ.get(_CONFIG_PATH_ENV),
            ],
            sort_keys=True,
            default=str,
        )
    except (TypeError, ValueError):
        # A configuration that cannot be rendered deterministically must not
        # be cached, because two requests cannot be proven equivalent.
        return None
    return hashlib.sha256(rendered.encode()).hexdigest()


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

    key: str | None = None
    if (
        copyable
        and _is_cacheable_registry(config)
        and outfile is None
        and not standalone
        and not overwrite
        and not searchPaths
    ):
        key = _cache_key(config, dimensionConfig, forceConfigRoot)

    if key is None:
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

    if key not in _templates:
        _stats["templates"] += 1
        holder = tempfile.mkdtemp(prefix="butler-repo-template-")
        _tmpdirs.append(holder)
        template_root = os.path.join(holder, "repo")
        template_config = Butler.makeRepo(
            template_root,
            config=config,
            dimensionConfig=dimensionConfig,
            forceConfigRoot=forceConfigRoot,
        )
        _templates[key] = (template_root, template_config)

    _stats["served"] += 1
    template_root, _ = _templates[key]
    destination = resource.ospath
    shutil.copytree(template_root, destination, dirs_exist_ok=True)
    # Read the config back from the copy so the caller sees its own root
    # rather than the template's.
    return Config(os.path.join(destination, "butler.yaml"))
