# -*- python -*-
from lsst.sconsUtils import scripts
from lsst.sconsUtils import state

scripts.BasicSConstruct("daf_butler", disableCc=True)
mypy = state.env.Command("mypy.log", "python/lsst/daf/butler",
                         "mypy python/lsst 2>&1 | tee -a mypy.log")
state.env.Alias("mypy", mypy)
