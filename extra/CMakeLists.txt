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
