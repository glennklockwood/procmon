## Discover dependencies

SET ( PROCMON_MONGODB_ROUSER   "mongo"                 CACHE STRING "" )
SET ( PROCMON_MONGODB_ROPASSWD ""                      CACHE STRING "" )

CONFIGURE_FILE( "househunter_app.py.in" "househunter_app.py" @ONLY )

INSTALL(FILES ${CMAKE_CURRENT_BINARY_DIR}/househunter_app.py DESTINATION share/procmon/webui
    PERMISSIONS OWNER_READ OWNER_EXECUTE GROUP_READ GROUP_EXECUTE WORLD_READ WORLD_EXECUTE)
