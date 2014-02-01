## Discover dependencies

SET ( MONITORING_AMQP_USER "monitoring" CACHE STRING "Name of rabbitmq monitoring user" )
SET ( MONITORING_AMQP_PASSWORD "XXXXXX" CACHE STRING "Password for rabbitmq monitoring user" )

CONFIGURE_FILE(
    "setup_rabbitmq_service.sh.in"
    "setup_rabbitmq_service.sh"
)

ADD_EXECUTABLE( timeout timeout/timeout.c )
    INSTALL_TARGETS(/test timeout)



INSTALL(FILES ${CMAKE_CURRENT_BINARY_DIR}/setup_rabbitmq_service.sh DESTINATION share/services)
ADD_SUBDIRECTORY( healthcheck )
