SET( AMQPLOGGER_AMQP_HOST ${DEFAULT_AMQP_HOST} CACHE STRING "amqpLogger rabbitmq server" )
SET( AMQPLOGGER_AMQP_VHOST ${DEFAULT_AMQP_VHOST} CACHE STRING "amqpLogger amqp virtual-host" )
SET( AMQPLOGGER_AMQP_PORT ${DEFAULT_AMQP_PORT} CACHE STRING "amqpLogger amqp port number" )
SET( AMQPLOGGER_AMQP_USER ${DEFAULT_AMQP_USER} CACHE STRING "amqpLogger amqp user" )
SET( AMQPLOGGER_AMQP_PASSWORD ${DEFAULT_AMQP_PASSWORD} CACHE STRING "amqpLogger amqp password" )
SET( AMQPLOGGER_AMQP_EXCHANGE ${DEFAULT_AMQP_EXCHANGE} CACHE STRING "amqpLogger amqp exchange" )
SET( AMQPLOGGER_AMQP_FRAMESIZE ${DEFAULT_AMQP_FRAMESIZE} CACHE STRING "amqpLogger maximum framesize in bytes" )
SET( AMQPLOGGER_AMQP_RETRIES 3 CACHE STRING "number of retries to send message" )
SET( AMQPLOGGER_SOCKET_PATH /var/run/amqplogger.sock CACHE PATH "path to amqplogger daemon socket" )
SET( AMQPLOGGER_SOCKET_BACKLOG 50 CACHE STRING "max number of queued connections" )

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

ADD_LIBRARY ( amqpLogger SHARED amqpLogger.c )
    SET_TARGET_PROPERTIES( amqpLogger PROPERTIES VERSION 1.0 )
    SET_TARGET_PROPERTIES( amqpLogger PROPERTIES SOVERSION 1.0 )
    TARGET_LINK_LIBRARIES( amqpLogger ${RABBITMQ_C_LIBRARIES} -pthread )

ADD_LIBRARY ( amqpLogger-static STATIC amqpLogger.c )
    SET_TARGET_PROPERTIES( amqpLogger-static PROPERTIES OUTPUT_NAME amqpLogger )
    TARGET_LINK_LIBRARIES( amqpLogger-static ${RABBITMQ_C_LIBRARIES} )

ADD_LIBRARY ( amqpLogger.fpic STATIC amqpLogger.c )
    SET_TARGET_PROPERTIES( amqpLogger.fpic PROPERTIES COMPILE_FLAGS "-fPIC")
    TARGET_LINK_LIBRARIES( amqpLogger ${RABBITMQ_C_LIBRARIES} )

INSTALL_TARGETS(/bin amqpLogger-bin)
INSTALL_TARGETS(/lib amqpLogger amqpLogger.fpic)
INSTALL(FILES amqpLogger.h DESTINATION include)

SET( AMQPLOGGER_LIBRARIES ${CMAKE_CURRENT_BINARY_DIR}/libamqpLogger.fpic.a )
SET( AMQPLOGGER_PYTHONDIR lib/python${PYTHON_VERSION_STRING}/site-packages/${AMQPLOGGER_NAME} )

ADD_SUBDIRECTORY( python )
ADD_SUBDIRECTORY( perl )
ADD_SUBDIRECTORY( tcl )
