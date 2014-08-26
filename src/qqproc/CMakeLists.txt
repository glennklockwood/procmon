FIND_PACKAGE(BISON REQUIRED)
FIND_PACKAGE(FLEX REQUIRED)

INCLUDE_DIRECTORIES ( "${CMAKE_CURRENT_SOURCE_DIR}" )
INCLUDE_DIRECTORIES ( "${CMAKE_CURRENT_BINARY_DIR}" )
INCLUDE_DIRECTORIES ( "${CMAKE_CURRENT_SOURCE_DIR}/.." )
INCLUDE_DIRECTORIES ( "${CMAKE_CURRENT_BINARY_DIR}/.." )

IF(BISON_FOUND)
    ADD_CUSTOM_COMMAND(
      OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/parser.c
      COMMAND ${BISON_EXECUTABLE}
              --defines=${CMAKE_CURRENT_BINARY_DIR}/parser.h
              --output=${CMAKE_CURRENT_BINARY_DIR}/parser.c
              ${CMAKE_CURRENT_SOURCE_DIR}/parser.y
      COMMENT "Generating parser.c"
    )
ENDIF()

IF(FLEX_FOUND)
    ADD_CUSTOM_COMMAND(
      OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/lexer.c
      COMMAND ${FLEX_EXECUTABLE}
              --header-file=${CMAKE_CURRENT_BINARY_DIR}/lexer.h
              --outfile=${CMAKE_CURRENT_BINARY_DIR}/lexer.c
              ${CMAKE_CURRENT_SOURCE_DIR}/lexer.l
      COMMENT "Generating lexer.c"
    )
ENDIF()

ADD_EXECUTABLE( qqproc client.cpp QProc.cpp lexer.c parser.c QProc.hh parseTree.hh Function.hh )
