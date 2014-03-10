## Discover dependencies

CONFIGURE_FILE( "firehose.genepool.cron.sh.in" "firehose.genepool.cron.sh" @ONLY )

INSTALL(FILES ${CMAKE_CURRENT_BINARY_DIR}/firehose.genepool.cron.sh DESTINATION analysis/cron
    PERMISSIONS OWNER_READ OWNER_EXECUTE GROUP_READ GROUP_EXECUTE WORLD_READ WORLD_EXECUTE)

