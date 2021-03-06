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

## Discover dependencies

OPTION ( USE_PROCMON_AMQP "Use amqp I/O for procmon" ON )
OPTION ( USE_PROCMON_HDF5 "Use hdf5 I/O for procmon" OFF )
OPTION ( USE_REDUCER_AMQP "Use amqp I/O for reducer" ON )
OPTION ( USE_REDUCER_HDF5 "Use hdf5 I/O for reducer" ON )
OPTION ( BUILD_ONLY_PROCMON "Only build the procmon sensor" OFF )
OPTION ( BUILD_STATIC_PROCMON "Build the procmon sensor statically" OFF )
OPTION ( BUILD_PROCMON_COLLECTION_TOOLS "Build the full procmon collection tools suite" ON )
OPTION ( PROCMON_DAEMONIZE_NOSETSID "Build procmon sensor to not call setsid" OFF )

OPTION ( Boost_USE_STATIC_LIBS ON  )
OPTION( Boost_USE_MULTITHREADED OFF )
set( _ORIG_CMAKE_FIND_LIBRARY_SUFFIXES ${CMAKE_FIND_LIBRARY_SUFFIXES})
set( CMAKE_FIND_LIBRARY_SUFFIXES .a )
FIND_PACKAGE( Boost REQUIRED COMPONENTS program_options filesystem system regex )
set( CMAKE_FIND_LIBRARY_SUFFIXES ${_ORIG_CMAKE_FIND_LIBRARY_SUFFIXES})

SET( CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -I${EXTRA_INCLUDEDIR}" )
SET( CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -L${EXTRA_LIBDIR}" )

IF ( BUILD_ONLY_PROCMON )
    SET ( BUILD_PROCMON_COLLECTION_TOOLS OFF )
ENDIF ( BUILD_ONLY_PROCMON )

SET ( PROCMON_FREQUENCY 30 CACHE STRING "Default number of seconds between procmon polling intervals" )
SET ( PROCMON_INTIAL_PHASE 30 CACHE STRING "Default number of seconds for altered polling frequency initial phase" )
SET ( PROCMON_INTIAL_PHASE_FREQUENCY 1 CACHE STRING "Default number of seconds between procmon polling intervals during initial phase" )
SET ( PROCMON_OUTPUT_FLAGS 4 CACHE STRING "Default output type, 1 for text, 2 for hdf5, 4 for amqp" )
SET ( PROCMON_IDENTIFIER "JOB_ID" CACHE STRING  "Default context identifier" )
SET ( PROCMON_SUBIDENTIFIER "SGE_TASK_ID" CACHE STRING "Default context subidentifier" )
SET ( PROCMON_AMQP_MESSAGE_RETRIES 1 CACHE STRING "Number of times for procmon sensor to retry message send" )

SET ( PROCREDUCER_H5_STAT_BLOCKSIZE 128 CACHE STRING "Number of procstat records per h5 block" )
SET ( PROCREDUCER_H5_DATA_BLOCKSIZE 4 CACHE STRING "Number of procdata records per h5 block" )
SET ( PROCREDUCER_H5_FD_BLOCKSIZE 64 CACHE STRING "Number of procfd records per h5 block" )
SET ( PROCREDUCER_H5_OBS_BLOCKSIZE 256 CACHE STRING "Number of procobs records per h5 block" )

SET ( PROCMUXER_HOSTNAME "*" CACHE STRING "Context hostname message selector" )
SET ( PROCMUXER_IDENTIFIER "*" CACHE STRING "Context identifier message selector" )
SET ( PROCMUXER_SUBIDENTIFIER "*" CACHE STRING "Context subidentifier message selector" )
SET ( PROCREDUCER_OUTPUT_PREFIX "procmonh5" CACHE STRING "Default output H5 file prefix" )
SET ( PROCREDUCER_CLEAN_FREQUENCY 600 CACHE STRING "(Obsolete) dump frequency for reducer hash table" )
SET ( PROCMUXER_MAX_PROCESS_AGE 3600 CACHE STRING "(Obsolete) maximum age in seconds for reducer hash table" )
SET ( PROCMUXER_MAX_FILE_WRITES 10000000 CACHE STRING "Maximum number of hdf5 writes before turning over h5 file (likely obsolete)" )
SET ( PROCMUXER_MAX_TOTAL_WRITES 100000000 CACHE STRING "Maximum number of hdf5 writes before exiting muxer process (likely obsolete)" )
SET ( PROCMUXER_MAX_MEMORY "5G" CACHE STRING "Maximum memory size of procmuxer" )
SET ( PROCREDUCER_MAX_FDS 10 CACHE STRING "Number of reported fds to expect on the wire" )

IF ( USE_HDF5 )
    FIND_PACKAGE( HDF5 )
ENDIF ( USE_HDF5 )

#FIND_PACKAGE(TBB)

CONFIGURE_FILE(
    "config.h.in"
    "config.h"
)

CONFIGURE_FILE(ProcIO.cpp ProcIO.procmon.cpp)
CONFIGURE_FILE(ProcIO.cpp ProcIO.reducer.cpp)
CONFIGURE_FILE(cray_run_procmon.c.in cray_run_procmon.c)

include( CheckCXXCompilerFlag )
check_cxx_compiler_flag(-static-libgcc CXX_HAS_STATIC_LIBGCC)
check_cxx_compiler_flag(-static-libstdc++ CXX_HAS_STATIC_LIBSTDCXX)

IF (BUILD_STATIC_PROCMON)
    set( CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -static" )
elseif (CXX_HAS_STATIC_LIBSTDCXX)
    set( CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -static-libgcc -static-libstdc++" )
#else ()
#    set( CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -static" )
endif ()


INCLUDE_DIRECTORIES ( "${CMAKE_CURRENT_SOURCE_DIR}" )
INCLUDE_DIRECTORIES ( "${CMAKE_CURRENT_BINARY_DIR}" )
INCLUDE_DIRECTORIES ( "${NCLQ_DIR}/include" )

#INCLUDE_DIRECTORIES ( "${TBB_INCLUDE_DIRS}" )

## Discover dependencies
INCLUDE_DIRECTORIES ( "${Boost_INCLUDE_DIRS}" )

IF ( USE_HDF5 )
    FIND_PACKAGE( HDF5 )
    INCLUDE_DIRECTORIES ( "${HDF5_INCLUDE_DIR}" )
ENDIF ( USE_HDF5 )

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


ADD_EXECUTABLE( procmon procmon.cpp ProcIO.procmon.cpp )
    SET_TARGET_PROPERTIES( procmon PROPERTIES COMPILE_FLAGS -DUSE_CONFIG_H=1 )
    TARGET_LINK_LIBRARIES( procmon ${Boost_LIBRARIES} )
INSTALL_TARGETS( /bin procmon)
#ADD_EXECUTABLE( cray_run_procmon cray_run_procmon.c )
#INSTALL_TARGETS( /bin cray_run_procmon )

IF ( PROCMON_DAEMONIZE_NOSETSID )
    append_compile_flags_to_target( procmon -DNOSETSID=1 )
ENDIF ( PROCMON_DAEMONIZE_NOSETSID )

IF ( BUILD_SECURED )
    ADD_EXECUTABLE( procmon.secured procmon.cpp ProcIO.procmon.cpp )
    SET_TARGET_PROPERTIES( procmon.secured PROPERTIES COMPILE_FLAGS -DUSE_CONFIG_H=1 )
    append_compile_flags_to_target( procmon.secured -DSECURED=1 )
    TARGET_LINK_LIBRARIES( procmon.secured pthread )
    TARGET_LINK_LIBRARIES( procmon.secured cap )
    TARGET_LINK_LIBRARIES( procmon.secured ${Boost_LIBRARIES} )
    INSTALL_TARGETS(/bin procmon.secured )
ENDIF ( BUILD_SECURED )

IF ( USE_PROCMON_HDF5 )
    append_compile_flags_to_target( procmon -DUSE_HDF5=1 )
    TARGET_LINK_LIBRARIES( procmon ${HDF5_LIBRARIES} )
    IF ( BUILD_SECURED )
        append_compile_flags_to_target( procmon.secured -DUSE_HDF5=1 )
        TARGET_LINK_LIBRARIES( procmon.secured ${HDF5_LIBRARIES} )
    ENDIF ( BUILD_SECURED )
ENDIF ( USE_PROCMON_HDF5 )
IF ( USE_PROCMON_AMQP )
    append_compile_flags_to_target( procmon -DUSE_AMQP=1 )
    TARGET_LINK_LIBRARIES( procmon ${RABBITMQ_C_LIBRARIES} "rt" )
    IF ( BUILD_SECURED )
        append_compile_flags_to_target( procmon.secured -DUSE_AMQP=1 )
        TARGET_LINK_LIBRARIES( procmon.secured ${RABBITMQ_C_LIBRARIES} "rt" )
    ENDIF ( BUILD_SECURED )
ENDIF ( USE_PROCMON_AMQP )

SET( CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11" )

IF ( BUILD_PROCMON_COLLECTION_TOOLS )
    ADD_EXECUTABLE( ProcReducer ProcReducer.cpp ProcIO.reducer.cpp ProcReducerData.cc )
        SET_TARGET_PROPERTIES( ProcReducer PROPERTIES COMPILE_FLAGS -DUSE_CONFIG_H=1 )
        TARGET_LINK_LIBRARIES( ProcReducer ${Boost_LIBRARIES} )
    ADD_EXECUTABLE( ProcMuxer ProcMuxer.cpp ProcIO.reducer.cpp )
        SET_TARGET_PROPERTIES( ProcMuxer PROPERTIES COMPILE_FLAGS -DUSE_CONFIG_H=1 )
        TARGET_LINK_LIBRARIES( ProcMuxer ${Boost_LIBRARIES} )
    ADD_EXECUTABLE( CheckH5 CheckH5.cpp ProcIO.reducer.cpp )
        SET_TARGET_PROPERTIES( CheckH5 PROPERTIES COMPILE_FLAGS -DUSE_CONFIG_H=1 )
        TARGET_LINK_LIBRARIES( CheckH5 ${Boost_LIBRARIES} )
    ADD_EXECUTABLE( PostReducer PostReducer.cpp ProcIO.reducer.cpp ProcReducerData.cc )
        SET_TARGET_PROPERTIES( PostReducer PROPERTIES COMPILE_FLAGS -DUSE_CONFIG_H=1 )
        TARGET_LINK_LIBRARIES( PostReducer ${Boost_LIBRARIES} )
    ADD_EXECUTABLE( jobtop jobtop.cpp ProcIO.procmon.cpp )
        SET_TARGET_PROPERTIES( jobtop PROPERTIES COMPILE_FLAGS -DUSE_CONFIG_H=1 )
        TARGET_LINK_LIBRARIES( jobtop ${Boost_LIBRARIES} )
    ADD_EXECUTABLE( LiveProcCache LiveProcCache.cpp ProcIO.procmon.cpp )
        SET_TARGET_PROPERTIES( LiveProcCache PROPERTIES COMPILE_FLAGS -DUSE_CONFIG_H=1 )
        TARGET_LINK_LIBRARIES( LiveProcCache ${Boost_LIBRARIES} "rt" )
    ADD_EXECUTABLE( ProcessSummarizer ProcessSummarizer.cpp ProcIO.reducer.cpp ProcessSummary.cpp ProcIO2.cpp)
        SET_TARGET_PROPERTIES( ProcessSummarizer PROPERTIES COMPILE_FLAGS -DUSE_CONFIG_H=1 )
        SET_TARGET_PROPERTIES( ProcessSummarizer PROPERTIES INSTALL_RPATH ${CMAKE_INSTALL_PREFIX}/lib )
        TARGET_LINK_LIBRARIES( ProcessSummarizer ${Boost_LIBRARIES} )
    ADD_EXECUTABLE( qqproc qqproc.cpp ProcIO2.cpp ProcessSummary.cpp )
        SET_TARGET_PROPERTIES( qqproc PROPERTIES COMPILE_FLAGS -DUSE_CONFIG_H=1 )
        TARGET_LINK_LIBRARIES( qqproc ${Boost_LIBRARIES} )
        TARGET_LINK_LIBRARIES( qqproc ${NCLQ_DIR}/lib/libnclq.a z)

    ADD_EXECUTABLE( DataCoverage DataCoverage.cpp ProcIO.reducer.cpp )
        SET_TARGET_PROPERTIES( DataCoverage PROPERTIES COMPILE_FLAGS -DUSE_CONFIG_H=1 )
        SET_TARGET_PROPERTIES( DataCoverage PROPERTIES INSTALL_RPATH ${CMAKE_INSTALL_PREFIX}/lib )
        TARGET_LINK_LIBRARIES( DataCoverage ${Boost_LIBRARIES} )
#        TARGET_LINK_LIBRARIES( ProcessSummarizer ${TBB_LIBRARY} )


    INSTALL_TARGETS(/sbin ProcReducer ProcMuxer CheckH5 PostReducer LiveProcCache)
    INSTALL_TARGETS(/bin jobtop ProcessSummarizer DataCoverage qqproc)

    IF ( USE_REDUCER_AMQP )
        append_compile_flags_to_target( CheckH5 -DUSE_AMQP=1 )
        TARGET_LINK_LIBRARIES( CheckH5 ${RABBITMQ_C_LIBRARIES} )
        append_compile_flags_to_target( ProcReducer -DUSE_AMQP=1 )
        TARGET_LINK_LIBRARIES( ProcReducer ${RABBITMQ_C_LIBRARIES} )
        append_compile_flags_to_target( PostReducer -DUSE_AMQP=1 )
        TARGET_LINK_LIBRARIES( PostReducer ${RABBITMQ_C_LIBRARIES} )
        append_compile_flags_to_target( ProcMuxer -DUSE_AMQP=1 )
        TARGET_LINK_LIBRARIES( ProcMuxer ${RABBITMQ_C_LIBRARIES} )
        append_compile_flags_to_target( jobtop -DUSE_AMQP=1 )
        TARGET_LINK_LIBRARIES( jobtop ${RABBITMQ_C_LIBRARIES} "ncurses" "rt" )
        append_compile_flags_to_target( LiveProcCache -DUSE_AMQP=1 )
        TARGET_LINK_LIBRARIES( LiveProcCache ${RABBITMQ_C_LIBRARIES} )
    ENDIF ( USE_REDUCER_AMQP )
    IF ( USE_REDUCER_HDF5 )
        append_compile_flags_to_target( CheckH5 -DUSE_HDF5=1 )
        TARGET_LINK_LIBRARIES( CheckH5 "rt" "dl" "z" ${HDF5_LIBRARIES} )
        append_compile_flags_to_target( ProcReducer -DUSE_HDF5=1 )
        TARGET_LINK_LIBRARIES( ProcReducer "rt" "dl" "z" ${HDF5_LIBRARIES} )
        append_compile_flags_to_target( PostReducer -DUSE_HDF5=1 )
        TARGET_LINK_LIBRARIES( PostReducer "rt" "dl" "z" ${HDF5_LIBRARIES} )
        append_compile_flags_to_target( ProcMuxer -DUSE_HDF5=1 )
        TARGET_LINK_LIBRARIES( ProcMuxer "rt" "dl" "z" ${HDF5_LIBRARIES} )
        append_compile_flags_to_target( ProcessSummarizer -DUSE_HDF5=1 )
        TARGET_LINK_LIBRARIES( ProcessSummarizer "rt" "dl" "z" ${HDF5_LIBRARIES} )
        append_compile_flags_to_target( DataCoverage -DUSE_HDF5=1 )
        TARGET_LINK_LIBRARIES( DataCoverage "rt" "dl" "z" ${HDF5_LIBRARIES} )
        append_compile_flags_to_target( qqproc -DUSE_HDF5=1 )
        TARGET_LINK_LIBRARIES( qqproc "rt" "dl" "z" ${HDF5_LIBRARIES} )
    ENDIF ( USE_REDUCER_HDF5 )
ENDIF ( BUILD_PROCMON_COLLECTION_TOOLS )

#ADD_SUBDIRECTORY( qp )
