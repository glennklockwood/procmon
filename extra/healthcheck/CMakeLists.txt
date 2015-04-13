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

SET ( MONITORING_AMQP_USER "monitoring" CACHE STRING "Name of rabbitmq monitoring user" )
SET ( MONITORING_AMQP_PASSWORD "XXXXXX" CACHE STRING "Password for rabbitmq monitoring user" )

CONFIGURE_FILE( "rabbitmq-alive.py.in" "rabbitmq-alive.py")
CONFIGURE_FILE( "rabbitmq-stats.py.in" "rabbitmq-stats.py")

INSTALL(FILES ${CMAKE_CURRENT_BINARY_DIR}/rabbitmq-alive.py DESTINATION share/healthchecks
    PERMISSIONS OWNER_READ OWNER_EXECUTE GROUP_READ GROUP_EXECUTE WORLD_READ WORLD_EXECUTE)
INSTALL(FILES ${CMAKE_CURRENT_BINARY_DIR}/rabbitmq-stats.py DESTINATION share/healthchecks
    PERMISSIONS OWNER_READ OWNER_EXECUTE GROUP_READ GROUP_EXECUTE WORLD_READ WORLD_EXECUTE)
INSTALL(FILES check_procmon_aggregation.pl DESTINATION share/healthchecks
    PERMISSIONS OWNER_READ OWNER_EXECUTE GROUP_READ GROUP_EXECUTE WORLD_READ WORLD_EXECUTE)
