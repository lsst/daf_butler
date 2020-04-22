"""Sphinx configuration file for an LSST stack package.

This configuration only affects single-package Sphinx documenation builds.
"""

from documenteer.sphinxconfig.stackconf import build_package_configs
import lsst.daf.butler
import lsst.daf.butler.version

_g = globals()
_g.update(build_package_configs(
    project_name="daf_butler",
    version=lsst.daf.butler.version.__version__))

extensions.append("sphinx_click.ext")  # noqa: F821
