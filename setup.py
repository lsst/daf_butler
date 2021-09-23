#!/usr/bin/env python
import os.path
import errno
import urllib.request
from setuptools import setup


version = "0.1.0"
with open("./python/lsst/daf/butler/version.py", "w") as f:
    print(f"""
__all__ = ("__version__", )
__version__ = '{version}'""", file=f)

# The purpose of this setup.py is to build enough of the system to
# allow testing. Not to distribute on PyPI.
# One impediment is the dependency on lsst.utils.
# if that package is missing we download the two files we need
# and include them in our distribution
urls = {}
try:
    import lsst.utils  # noqa: F401
except ImportError:
    urls = {"doImport.py":
            "https://raw.githubusercontent.com/lsst/utils/master/python/lsst/utils/doImport.py",
            "tests.py":
            "https://raw.githubusercontent.com/lsst/utils/master/python/lsst/utils/tests.py"}

if urls:
    utils_dir = "python/lsst/utils"
    if not os.path.exists(utils_dir):
        try:
            os.makedirs(utils_dir)
        except OSError as e:
            # Don't fail if directory exists due to race
            if e.errno != errno.EEXIST:
                raise e
    failed = False
    for file, url in urls.items():
        outpath = os.path.join(utils_dir, file)
        # Do not redownload if the file is there
        if os.path.exists(outpath):
            continue
        try:
            contents = urllib.request.urlopen(url).read()
            print(f"Successfully read from {url}: {len(contents)} bytes ({type(contents)})")
            if isinstance(contents, bytes):
                contents = contents.decode()
        except Exception as e:
            print(f"Unable to download url {url}: {e}")
            failed = True
        else:
            with open(outpath, "w") as fh:
                print(contents, file=fh)
    # Write a simple __init__.py
    if not failed:
        init_path = os.path.join(utils_dir, "__init__.py")
        if not os.path.exists(init_path):
            with open(init_path, "w") as fh:
                print("from .doImport import *", file=fh)

# read the contents of our README file
this_directory = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(this_directory, "README.md"), encoding='utf-8') as f:
    long_description = f.read()

setup(
    version=f"{version}",
    long_description=long_description,
    long_description_content_type="text/markdown"
)
