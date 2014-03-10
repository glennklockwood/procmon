## Discover dependencies

SET ( PROCMON_MONGODB_HOST   "localhost"             CACHE STRING "" )
SET ( PROCMON_MONGODB_PORT   27017                   CACHE STRING "" )
SET ( PROCMON_MONGODB_USER   "mongo"                 CACHE STRING "" )
SET ( PROCMON_MONGODB_PASSWD ""                      CACHE STRING "" )

CONFIGURE_FILE( "procHunter.sh.in" "procHunter.sh" @ONLY )
CONFIGURE_FILE( "catjob.in" "catjob" @ONLY )
CONFIGURE_FILE( "mongoSummary.py.in" "mongoSummary.py" @ONLY )
CONFIGURE_FILE( "mongoSummary.sh.in" "mongoSummary.sh" @ONLY )

INSTALL(FILES catjob.py procFinder.py ${CMAKE_CURRENT_BINARY_DIR}/mongoSummary.py DESTINATION analysis
    PERMISSIONS OWNER_READ OWNER_EXECUTE GROUP_READ GROUP_EXECUTE WORLD_READ WORLD_EXECUTE)

INSTALL(FILES ${CMAKE_CURRENT_BINARY_DIR}/catjob ${CMAKE_CURRENT_BINARY_DIR}/procHunter.sh ${CMAKE_CURRENT_BINARY_DIR}/mongoSummary.sh DESTINATION analysis
    PERMISSIONS OWNER_READ OWNER_EXECUTE GROUP_READ GROUP_EXECUTE WORLD_READ WORLD_EXECUTE)

ADD_SUBDIRECTORY( cron )
ADD_SUBDIRECTORY( python )