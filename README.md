# lsst-daf-butler

[![pypi](https://img.shields.io/pypi/v/lsst-daf-butler.svg)](https://pypi.org/project/lsst-daf-butler/)
[![codecov](https://codecov.io/gh/lsst/daf_butler/branch/main/graph/badge.svg?token=2BUBL8R9RH)](https://codecov.io/gh/lsst/daf_butler)

LSST Data Access framework described in [arXiv:2206.14941](https://arxiv.org/abs/2206.14941).
Please cite [the SPIE paper](https://doi.org/10.1117/12.2629569) when using this software.

This is a **Python 3 only** package (we assume Python 3.11 or higher).

* SPIE paper from 2024: [Converting Rubin Observatory's data butler to a client/server architecture](https://doi.org/10.1117/12.3019130) ([Tech note](https://dmtn-288.lsst.io))
* SPIE Paper from 2022: [The Vera C. Rubin Observatory Data Butler and Pipeline Execution System](https://doi.org/10.1117/12.2629569) ([arXiv](https://arxiv.org/abs/2206.14941)) \[Primary reference]
* ADASS paper from 2019: [Abstracting the Storage and Retrieval of Image Data at the LSST](https://ui.adsabs.harvard.edu/abs/2019ASPC..523..653J/abstract).
* Early design note: [DMTN-056](https://dmtn-056.lsst.io)

PyPI: [lsst-daf-butler](https://pypi.org/project/lsst-daf-butler/)

This software is dual licensed under the GNU General Public License (version 3 of the License, or (at your option) any later version, and also under a 3-clause BSD license).
Recipients may choose which of these licenses to use; please see the files gpl-3.0.txt and/or bsd_license.txt, respectively.

## Arrow Memory Leaks

From version 18 of arrow we have seen significant memory leaks when accessing parquet files using the default memory allocator.
If you see such leaks the workaround is to set the `ARROW_DEFAULT_MEMORY_POOL` environment variable to `jemalloc` following the advice from [apache/arrow#45882](https://github.com/apache/arrow/issues/45882).
For EUPS users this variable is automatically set.

## Managing dependencies

This package uses [uv](https://docs.astral.sh/uv/) to manage the versions of libraries used for CI, Docker builds, and local development.

### Upgrading the versions of libraries

Run the following command to refresh `uv.lock` with the latest available
versions of all libraries:
```
uv lock --upgrade
```

### Temporarily pinning another LSST package to a development branch
If you are working on a PR that requires unreleased changes from another LSST
repository, you can temporarily pin the new version by opening `pyproject.toml`
and finding the library in the `override-dependencies` declaration under
`tool.uv`.  Replace `main` with the ticket branch, e.g.:
```
override-dependencies = [
    "lsst-utils @ git+https://github.com/lsst/utils@tickets/DM-xxxxx",
    ...
]
```

Then run:
```
uv lock --upgrade-package <package name>
```
to pull in the updated version for testing, where `<package name>` is
`lsst-utils` or whatever package you are overriding.
 (You can also just do `uv lock --upgrade` if you don't mind upgrading all the
 other libraries at the same time.)

You will need to re-do this lock update any time you change the code, so that
it resolves to the new version.

After the other library's development branch has been merged, reset the
branch to main in `pyproject.toml` and run the lock update again to pull in
the released version.
