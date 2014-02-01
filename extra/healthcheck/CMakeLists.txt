## Discover dependencies

SET ( MONITORING_AMQP_USER "monitoring" CACHE STRING "Name of rabbitmq monitoring user" )
SET ( MONITORING_AMQP_PASSWORD "XXXXXX" CACHE STRING "Password for rabbitmq monitoring user" )

CONFIGURE_FILE(
    "rabbitmq-alive.py.in"
    "rabbitmq-alive.py"
)


INSTALL(FILES ${CMAKE_CURRENT_BINARY_DIR}/rabbitmq-alive.py DESTINATION share/healthchecks)
