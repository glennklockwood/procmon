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

cmake_minimum_required (VERSION 2.8)

PROJECT( procmon )
SET( PROCMON_VERSION 2.6 )


OPTION ( USE_RABBITMQ_C "Use rabbitmq-c" ON )
OPTION ( USE_ZEROMQ "Use zeromq" ON )
OPTION ( USE_HDF5 "Use HDF5 Libraries" ON )
OPTION ( BUILD_PROCMON "Build procmon" ON )
OPTION ( BUILD_AMQPLOGGER "Build the amqpLogger" ON )
OPTION ( BUILD_EXTRA "Build the extra components" ON )


SET( CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -O3 -ggdb" )
SET( CMAKE_C_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG} -ggdb" )
SET( CMAKE_C_FLAGS_RELEASE "${CMAKE_C_FLAGS_RELEASE} -g -ggdb" )
SET( CMAKE_CXX_FLAGS "${CMAKE_C_FLAGS} -O3 -ggdb" )
SET( CMAKE_CXX_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG} -ggdb" )
SET( CMAKE_CXX_FLAGS_RELEASE "${CMAKE_C_FLAGS_RELEASE} -g -ggdb" )

OPTION( BUILD_SECURED "Build the secured version for root execution" ON )
SET( DEFAULT_AMQP_HOST "unknown.rabbit.mq" CACHE STRING "Default rabbitmq server" )
SET( DEFAULT_AMQP_VHOST "/" CACHE STRING "Default amqp virtual-host" )
SET( DEFAULT_AMQP_PORT  5672 CACHE STRING "Default amqp port number" )
SET( DEFAULT_AMQP_USER  "procmon" CACHE STRING "Default amqp user" )
SET( DEFAULT_AMQP_PASSWORD "invalid" CACHE STRING "Default amqp password" )
SET( DEFAULT_AMQP_EXCHANGE "procmon" CACHE STRING "Default amqp exchange" )
SET( DEFAULT_AMQP_FRAMESIZE 131072 CACHE STRING "Default maximum framesize in bytes" )

SET(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/CMake")


## get a private and patched installation of rabbitmq 0.4.1
IF ( USE_RABBITMQ_C )
    SET( RABBITMQ_C_DIR "${CMAKE_BINARY_DIR}/rabbit" )
    INCLUDE( ExternalProject )
    ExternalProject_Add(
        rabbitmq-0.5.2
        URL https://github.com/alanxz/rabbitmq-c/releases/download/v0.5.2/rabbitmq-c-0.5.2.tar.gz
        PATCH_COMMAND patch -p1 < ${CMAKE_SOURCE_DIR}/extra/rabbitmq-c/fpic_static.patch
        CMAKE_ARGS -DCMAKE_INSTALL_PREFIX=${RABBITMQ_C_DIR} -DBUILD_STATIC_LIBS=ON -DBUILD_SHARED_LIBS=OFF -DBUILD_FPICSTATIC_LIBS=ON -DENABLE_SSL_SUPPORT=OFF
        )
    SET( RABBITMQ_C_INCLUDE_DIR ${RABBITMQ_C_DIR}/include )
    SET( RABBITMQ_C_LIB_DIR ${RABBITMQ_C_DIR}/lib )
    SET( RABBITMQ_C_LIBRARIES "${RABBITMQ_C_DIR}/lib/librabbitmq.fpic.a" )
ENDIF ( USE_RABBITMQ_C )
#SET (TBB_DIR "${CMAKE_BINARY_DIR}/tbb" )
#ExternalProject_Add (
#    tbb-4.3
#    URL https://www.threadingbuildingblocks.org/sites/default/files/software_releases/source/tbb43_20140724oss_src.tgz
#    CONFIGURE_COMMAND ""
#    BUILD_COMMAND make
#    BUILD_IN_SOURCE 1
#    INSTALL_COMMAND ""
#)
#ExternalProject_Get_Property(tbb-4.3 source_dir)
#FILE (GLOB ALL_TBB_LIBRARY_FILES "${source_dir}/build/*release/*.so*")
#INSTALL_FILES ( lib FILES ${ALL_TBB_LIBRARY_FILES} )
#
#SET (TBB_INCLUDE_DIRS ${source_dir}/include)
#FILE (GLOB TBB_LIBRARY_FILE "${source_dir}/build/*release/libtbb.so")
#SET (TBB_LIBRARY ${TBB_LIBRARY_FILE})

#MESSAGE( "TBB_LIBRARY: ${TBB_LIBRARY}" )


#IF ( USE_ZEROMQ ) 
#    SET( ZEROMQ_DIR "${CMAKE_BINARY_DIR}/zeromq" )
#    INCLUDE( ExternalProject )
#    ExternalProject_Add(
#        zeromq-4.0.4
#        URL http://download.zeromq.org/zeromq-4.0.4.tar.gz
#        CONFIGURE_COMMAND ./configure --prefix=${ZEROMQ_DIR} --disable-dependency-tracking --enable-static --disable-shared --with-pic
#        PREFIX ${ZEROMQ_DIR}
#        BUILD_COMMAND make
#        BUILD_IN_SOURCE 1
#    )
#ENDIF ( USE_ZEROMQ )

## setup nclq
FIND_PROGRAM(VALGRIND_EXECUTABLE NAMES valgrind DOC "path to the valgrind executable")
FIND_PATH(VALGRIND_INCLUDE_DIR valgrind/memcheck.h DOC "path to the valgrind headers")
SET (FLEX_DIR $ENV{FLEX_DIR})
SET (NCLQ_DIR ${CMAKE_BINARY_DIR}/nclq)
ExternalProject_Add(
    nclq
    GIT_REPOSITORY https://bitbucket.org/berkeleylab/nersc-nclq.git
    CMAKE_ARGS "-DFLEX_INCLUDE_DIR=${FLEX_DIR}/include"
        "-DVALGRIND_INCLUDE_DIR=${VALGRIND_DIR}/include"
        "-DCMAKE_INSTALL_PREFIX=${NCLQ_DIR}"
        "-DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}"
    TEST_BEFORE_INSTALL 1
)

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
