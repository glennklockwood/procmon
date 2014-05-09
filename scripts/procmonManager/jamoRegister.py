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
