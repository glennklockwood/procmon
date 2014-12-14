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

SET ( PROCMON_MONGODB_ROUSER   "mongo"                 CACHE STRING "" )
SET ( PROCMON_MONGODB_ROPASSWD ""                      CACHE STRING "" )

CONFIGURE_FILE( "househunter_app.py.in" "househunter_app.py" @ONLY )

INSTALL(FILES ${CMAKE_CURRENT_BINARY_DIR}/househunter_app.py DESTINATION share/procmon/webui
    PERMISSIONS OWNER_READ OWNER_EXECUTE GROUP_READ GROUP_EXECUTE WORLD_READ WORLD_EXECUTE)
