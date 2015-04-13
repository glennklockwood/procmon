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

FIND_PACKAGE( SWIG REQUIRED )
INCLUDE( ${SWIG_USE_FILE} )
FIND_PACKAGE( PerlLibs )
FIND_PACKAGE( PythonLibs )
FIND_PACKAGE( PythonInterp )
FIND_PACKAGE( TCL )

CONFIGURE_FILE( "config.h.in" "config.h")

SET( CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11" )
SET( CMAKE_EXE_LINKER_FLAGS "-static-libgcc -static-libstdc++" )

INCLUDE_DIRECTORIES ( "${CMAKE_CURRENT_SOURCE_DIR}" )
INCLUDE_DIRECTORIES ( "${CMAKE_CURRENT_BINARY_DIR}" )

IF ( USE_RABBITMQ_C )
    INCLUDE_DIRECTORIES ( "${RABBITMQ_C_INCLUDE_DIR}" )
ENDIF ( USE_RABBITMQ_C )

    function(append_compile_flags_to_target _target)
       get_target_property(_existing ${_target} COMPILE_FLAGS)
       if(_existing)
           set_target_properties(${_target} PROPERTIES COMPILE_FLAGS "${_existing} ${ARGN}")
              else()
           set_target_properties(${_target} PROPERTIES COMPILE_FLAGS ${ARGN})
       endif()
    endfunction()


ADD_EXECUTABLE( amqpLogger-bin main.c amqpLogger.c )
    SET_TARGET_PROPERTIES( amqpLogger-bin PROPERTIES COMPILE_FLAGS -DUSE_CONFIG_H=1 )
    SET_TARGET_PROPERTIES( amqpLogger-bin PROPERTIES OUTPUT_NAME amqpLogger )
    TARGET_LINK_LIBRARIES( amqpLogger-bin ${RABBITMQ_C_LIBRARIES} )
    TARGET_LINK_LIBRARIES( amqpLogger-bin "rt" )
    TARGET_LINK_LIBRARIES( amqpLogger-bin "pthread" )

ADD_LIBRARY ( amqpLogger SHARED amqpLogger.c )
    SET_TARGET_PROPERTIES( amqpLogger PROPERTIES VERSION 1.0 )
    SET_TARGET_PROPERTIES( amqpLogger PROPERTIES SOVERSION 1.0 )
    TARGET_LINK_LIBRARIES( amqpLogger ${RABBITMQ_C_LIBRARIES} )

ADD_LIBRARY ( amqpLogger-static STATIC amqpLogger.c )
    SET_TARGET_PROPERTIES( amqpLogger-static PROPERTIES OUTPUT_NAME amqpLogger )
    TARGET_LINK_LIBRARIES( amqpLogger-static ${RABBITMQ_C_LIBRARIES} )

ADD_LIBRARY ( amqpLogger.fpic STATIC amqpLogger.c )
    SET_TARGET_PROPERTIES( amqpLogger.fpic PROPERTIES COMPILE_FLAGS "-fPIC")
    TARGET_LINK_LIBRARIES( amqpLogger.fpic ${RABBITMQ_C_LIBRARIES} )

INSTALL_TARGETS(/bin amqpLogger-bin)
INSTALL_TARGETS(/lib amqpLogger amqpLogger.fpic)
INSTALL(FILES amqpLogger.h DESTINATION include)

SET( AMQPLOGGER_LIBRARIES ${CMAKE_CURRENT_BINARY_DIR}/libamqpLogger.fpic.a )
if ( PYTHONLIBS_FOUND ) 
    SET( AMQPLOGGER_PYTHONDIR lib/python${PYTHON_VERSION_STRING}/site-packages/${AMQPLOGGER_NAME} )
    ADD_SUBDIRECTORY( python )
endif ()
ADD_SUBDIRECTORY( perl )
ADD_SUBDIRECTORY( tcl )
