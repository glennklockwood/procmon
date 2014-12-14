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
SET ( MONITORING_AMQP_PORT "15672"         CACHE STRING "port for rabbitmq monitoring" )

CONFIGURE_FILE(
    "setup_rabbitmq_service.sh.in"
    "setup_rabbitmq_service.sh"
)

ADD_EXECUTABLE( timeout timeout/timeout.c )
    INSTALL_TARGETS(/test timeout)

INSTALL(FILES ${CMAKE_CURRENT_BINARY_DIR}/setup_rabbitmq_service.sh DESTINATION share/services
    PERMISSIONS OWNER_READ OWNER_EXECUTE GROUP_READ GROUP_EXECUTE WORLD_READ WORLD_EXECUTE)
ADD_SUBDIRECTORY( healthcheck )
