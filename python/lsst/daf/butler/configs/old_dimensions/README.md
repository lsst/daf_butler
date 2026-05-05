These files are used by
[daf_butler_migrate](https://github.com/lsst-dm/daf_butler_migrate) to verify
and update the dimension universe stored in the database when upgrading the
database schema.

After you updated `dimensions.yaml` and and gave it a new version version, copy it to this directory with a new name.
The naming convention is `<namespace>_universe<version>.yaml`.
