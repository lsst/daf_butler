# Python library requirements

The files in this directory are used to create `requirements.txt` files for installing Python libraries via `pip`.
There are two types of files here:
* `.in` files are unpinned requirements with very loose or unspecified version numbers for the libraries.
These files include only the top-level required libraries, and not their implied dependencies.
* `.txt` files are generated from the `.in` files using `pip-compile` from [pip-tools](https://github.com/jazzband/pip-tools).
These are fully-resolved requirements files that specify an exact version for each library and all of their child dependencies.
It also includes a hash of the library's archive file, which will cause `pip` to raise a security error if the library changes unexpectedly.

## Specifying dependencies for GitHub actions
GitHub actions use `requirements.txt` from the root of the repository.
That `requirements.txt` file points to the `.in` files so it gets unpinned dependencies.
This causes us to always build with the latest dependencies available on PyPI.[^1]

[^1]: (DI: I personally find it frustrating to randomly get build failures unrelated to your PR, but the rest of the team likes it.)

## Specifying dependencies for the Docker container
The Docker container uses fully-pinned requirements from two files:
* `docker.txt` -- libraries built into the final server container
* `docker-test.txt` -- additional libraries installed only for running unit tests

### Adding or upgrading dependencies used for the Docker container
Run `upgrade-deps.sh` from this directory to re-compile the `.txt` files from the `.in` files.
This will check PyPI for updated versions of libraries and update the `.txt` files with anything it finds.

### Using unreleased versions of LSST libraries
`pip-compile` does not support using a Git repo as an install source for a pinned version of a library.
(It doesn't know how to convert from a branch name to a commit hash, and it doesn't know how to generate a hash for pip.)

However, [Github supports generating a (mostly) immutable tarball from a commit hash](https://docs.github.com/en/repositories/working-with-files/using-files/downloading-source-code-archives#source-code-archive-urls),
so it is possible to point to an unreleased version of a library if you have a PR that requires updated code from one of the LSST libraries.

To do this, specify the library in the `docker.in` file using a URL in the form:
```
https://github.com/<repo>/archive/<commit hash>.tar.gz
```
example:
```
https://github.com/lsst/resources/archive/72166a0b0c217ef8c004cdb223423833a5fdfe88.tar.gz
```

Note that Github will allow you to use a branch name instead of a specific commit hash, but you should not do this.
If you use a branch name, the data inside the archive will change when new commits are pushed.
This will cause pip's integrity check to fail, and it will refuse to install the library.
