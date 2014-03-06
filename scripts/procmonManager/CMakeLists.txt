## Discover dependencies

SET ( MANAGER_GROUP         "procman_dev"            CACHE STRING "" )
SET ( MANAGER_SCRATCH       "/tmp/procmon_dev"       CACHE STRING "" )
SET ( PROCMON_H5_STORAGE    "/tmp/procmon"           CACHE STRING "" )
SET ( PROCMON_H5_SCRATCH    "/tmp/procmon_tmp"       CACHE STRING "" )
SET ( MANAGER_EMAIL         "root@localhost"         CACHE STRING "" )
SET ( MANAGER_TARGET_EMAIL  "dmj@localhost"          CACHE STRING "" )
SET ( USE_JAMO              "False"                  CACHE STRING "" )
SET ( JAMO_URL              "https://none.localhost" CACHE STRING "" )
SET ( JAMO_TOKEN            "XXXXX"                  CACHE STRING "" )
SET ( JAMO_USER             "nobody"                 CACHE STRING "" )
SET ( MANAGER_USE_EMAIL     "True"                   CACHE STRING "" )


CONFIGURE_FILE( "procmonManager.conf.in" "procmonManager.conf" )

INSTALL(FILES ${CMAKE_CURRENT_BINARY_DIR}/procmonManager.conf DESTINATION etc
    PERMISSIONS OWNER_READ OWNER_WRITE GROUP_READ WORLD_READ )
INSTALL(FILES procmonManager.py DESTINATION sbin
    PERMISSIONS OWNER_READ OWNER_EXECUTE GROUP_READ GROUP_EXECUTE WORLD_READ WORLD_EXECUTE)
