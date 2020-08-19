# -*- coding: utf-8 -*-

"""The initialization module to add the submodules to the python path."""

import os
import sys

from sub_modules import SUB_MODULES

for sub_module in SUB_MODULES:
    library_path = os.path.realpath(sub_module)
    if library_path not in sys.path:
        sys.path.append(library_path)
