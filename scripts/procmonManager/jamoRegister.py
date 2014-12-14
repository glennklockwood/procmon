################################################################################
# procmon, Copyright (c) 2014, The Regents of the University of California,
# through Lawrence Berkeley National Laboratory (subject to receipt of any
# required approvals from the U.S. Dept. of Energy).  All rights reserved.
#
# If you have questions about your rights to use or distribute this software,
# please contact Berkeley Lab's Technology Transfer Department at  TTD@lbl.gov.
#
# The LICENSE file in the root directory of the source code archive describes
# the licensing and distribution rights and restrictions on this software.
#
# Author:   Douglas Jacobsen <dmj@nersc.gov>
################################################################################

#!/usr/bin/env python

import os
import sys
import threading
import procmonManager


if __name__ == "__main__":
    (config, remaining_args) = procmonManager.read_configuration(sys.argv[1:])
    if config.use_jamo:
        import sdm_curl
        config.sdm      = sdm_curl.Curl(config.jamo_url, appToken=config.jamo_token)
        config.sdm_lock = threading.Lock()

    for fname in remaining_args:
        procmonManager.register_jamo(config, fname, "procmon_reduced_h5", sources=None)
