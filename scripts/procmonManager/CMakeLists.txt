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

## Discover dependencies

SET ( MANAGER_GROUP         "procman_dev"            CACHE STRING "" )
SET ( MANAGER_SCRATCH       "/tmp/procmon_dev"       CACHE STRING "" )
SET ( PROCMON_H5_STORAGE    "/tmp/procmon"           CACHE STRING "" )
SET ( PROCMON_H5_SCRATCH    "/tmp/procmon_tmp"       CACHE STRING "" )
SET ( MANAGER_EMAIL         "root@localhost"         CACHE STRING "" )
SET ( MANAGER_TARGET_EMAIL  "dmj@localhost"          CACHE STRING "" )
SET ( USE_JAMO              "False"                  CACHE STRING "" )
SET ( USE_HPSS              "False"                  CACHE STRING "" )
SET ( JAMO_URL              "https://none.localhost" CACHE STRING "" )
SET ( JAMO_TOKEN            "XXXXX"                  CACHE STRING "" )
SET ( JAMO_USER             "nobody"                 CACHE STRING "" )
SET ( MANAGER_USE_EMAIL     "True"                   CACHE STRING "" )


CONFIGURE_FILE( "procmonManager.conf.in" "procmonManager.conf" )

INSTALL(FILES ${CMAKE_CURRENT_BINARY_DIR}/procmonManager.conf DESTINATION etc
    PERMISSIONS OWNER_READ OWNER_WRITE GROUP_READ WORLD_READ )
INSTALL(FILES procmonManager.py DESTINATION sbin
    PERMISSIONS OWNER_READ OWNER_EXECUTE GROUP_READ GROUP_EXECUTE WORLD_READ WORLD_EXECUTE)
