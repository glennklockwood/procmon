## Discover dependencies

SET ( MONITORING_AMQP_USER "monitoring" CACHE STRING "Name of rabbitmq monitoring user" )
SET ( MONITORING_AMQP_PASSWORD "XXXXXX" CACHE STRING "Password for rabbitmq monitoring user" )

CONFIGURE_FILE( "rabbitmq-alive.py.in" "rabbitmq-alive.py")
CONFIGURE_FILE( "rabbitmq-stats.py.in" "rabbitmq-stats.py")

INSTALL(FILES ${CMAKE_CURRENT_BINARY_DIR}/rabbitmq-alive.py DESTINATION share/healthchecks
    PERMISSIONS OWNER_READ OWNER_EXECUTE GROUP_READ GROUP_EXECUTE WORLD_READ WORLD_EXECUTE)
INSTALL(FILES ${CMAKE_CURRENT_BINARY_DIR}/rabbitmq-stats.py DESTINATION share/healthchecks
    PERMISSIONS OWNER_READ OWNER_EXECUTE GROUP_READ GROUP_EXECUTE WORLD_READ WORLD_EXECUTE)