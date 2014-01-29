cmake_minimum_required (VERSION 2.8)

PROJECT( procmon )
SET( PROCMON_VERSION 2.5 )

OPTION ( USE_RABBITMQ_C "Use rabbitmq-c" ON )
OPTION ( USE_HDF5 "Use HDF5 Libraries" ON )
OPTION ( USE_BOOST "Use Boost Libraries" ON )

SET( CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -O2" )
SET( CMAKE_C_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG} -ggdb" )
SET( CMAKE_C_FLAGS_RELEASE "${CMAKE_C_FLAGS_RELEASE} -O2" )
SET( CMAKE_CXX_FLAGS "${CMAKE_C_FLAGS} -O2" )
SET( CMAKE_CXX_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG} -ggdb" )
SET( CMAKE_CXX_FLAGS_RELEASE "${CMAKE_C_FLAGS_RELEASE} -O2" )

OPTION( BUILD_SECURED "Build the secured version for root execution" OFF)
SET( DEFAULT_AMQP_HOST "unknown.rabbit.mq" CACHE STRING "default rabbitmq server" FORCE)

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
ENDIF ( USE_RABBITMQ_C )

ADD_SUBDIRECTORY( src )
#INCLUDE_DIRECTORIES("${PROJECT_BINARY_DIR}")
#
#SET( procmon_SRCS procmon.cpp )
#ADD_EXECUTABLE( procmon ${procmon_SRCS} )

