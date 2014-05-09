cmake_minimum_required (VERSION 2.8)

PROJECT( procmon )
SET( PROCMON_VERSION 2.5 )

OPTION ( USE_RABBITMQ_C "Use rabbitmq-c" ON )
OPTION ( USE_HDF5 "Use HDF5 Libraries" ON )
OPTION ( USE_BOOST "Use Boost Libraries" ON )
OPTION ( BUILD_PROCMON "Build procmon" ON )
OPTION ( BUILD_AMQPLOGGER "Build the amqpLogger" ON )
OPTION ( BUILD_EXTRA "Build the extra components" ON )

SET( CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -O2" )
SET( CMAKE_C_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG} -ggdb" )
SET( CMAKE_C_FLAGS_RELEASE "${CMAKE_C_FLAGS_RELEASE} -O2" )
SET( CMAKE_CXX_FLAGS "${CMAKE_C_FLAGS} -O2" )
SET( CMAKE_CXX_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG} -ggdb" )
SET( CMAKE_CXX_FLAGS_RELEASE "${CMAKE_C_FLAGS_RELEASE} -O2" )

OPTION( BUILD_SECURED "Build the secured version for root execution" ON )
SET( DEFAULT_AMQP_HOST "unknown.rabbit.mq" CACHE STRING "Default rabbitmq server" )
SET( DEFAULT_AMQP_VHOST "/" CACHE STRING "Default amqp virtual-host" )
SET( DEFAULT_AMQP_PORT  5672 CACHE STRING "Default amqp port number" )
SET( DEFAULT_AMQP_USER  "procmon" CACHE STRING "Default amqp user" )
SET( DEFAULT_AMQP_PASSWORD "invalid" CACHE STRING "Default amqp password" )
SET( DEFAULT_AMQP_EXCHANGE "procmon" CACHE STRING "Default amqp exchange" )
SET( DEFAULT_AMQP_FRAMESIZE 131072 CACHE STRING "Default maximum framesize in bytes" )

IF ( USE_BOOST )
    SET( Boost_USE_STATIC_LIBS ON )
    SET( Boost_USE_MULTITHREADED OFF )
    SET( Boost_USE_STATIC_RUNTIME ON )
    FIND_PACKAGE( Boost 1.50.0 COMPONENTS program_options )
ENDIF ( USE_BOOST )

## get a private and patched installation of rabbitmq 0.4.1
IF ( USE_RABBITMQ_C )
    SET( RABBITMQ_C_DIR "${CMAKE_BINARY_DIR}/rabbit" )
    INCLUDE( ExternalProject )
    ExternalProject_Add(
        rabbitmq-0.4.1
        URL http://github.com/alanxz/rabbitmq-c/releases/download/v0.4.1/rabbitmq-c-0.4.1.tar.gz
        PATCH_COMMAND patch -p1 < ${CMAKE_SOURCE_DIR}/extra/rabbitmq-c/fpic_static.patch
        CMAKE_ARGS -DCMAKE_INSTALL_PREFIX=${RABBITMQ_C_DIR} -DBUILD_STATIC_LIBS=ON -DBUILD_SHARED_LIBS=ON -DBUILD_FPICSTATIC_LIBS=ON -DENABLE_SSL_SUPPORT=OFF
        )
    SET( RABBITMQ_C_INCLUDE_DIR ${RABBITMQ_C_DIR}/include )
    SET( RABBITMQ_C_LIB_DIR ${RABBITMQ_C_DIR}/lib )
    SET( RABBITMQ_C_LIBRARIES "${RABBITMQ_C_DIR}/lib/librabbitmq.fpic.a" )
ENDIF ( USE_RABBITMQ_C )

IF ( BUILD_PROCMON )
    ADD_SUBDIRECTORY( src )
ENDIF ( BUILD_PROCMON )

IF ( BUILD_AMQPLOGGER )
    ADD_SUBDIRECTORY( amqpLogger )
ENDIF ( BUILD_AMQPLOGGER )

IF ( BUILD_EXTRA )
    ADD_SUBDIRECTORY( extra )
    ADD_SUBDIRECTORY( scripts )
    ADD_SUBDIRECTORY( webui )
ENDIF ( BUILD_EXTRA )
