## Discover dependencies

OPTION ( USE_PROCMON_AMQP "Use amqp I/O for procmon" ON )
OPTION ( USE_PROCMON_HDF5 "Use hdf5 I/O for procmon" OFF )
OPTION ( USE_REDUCER_AMQP "Use amqp I/O for reducer" ON )
OPTION ( USE_REDUCER_HDF5 "Use hdf5 I/O for reducer" ON )

IF ( USE_BOOST )
    SET( Boost_USE_STATIC_LIBS ON )
    SET( Boost_USE_MULTITHREADED OFF )
    SET( Boost_USE_STATIC_RUNTIME ON )
    FIND_PACKAGE( Boost 1.54.0 COMPONENTS program_options )
ENDIF ( USE_BOOST )

IF ( USE_HDF5 )
    FIND_PACKAGE( HDF5 )
ENDIF ( USE_HDF5 )

CONFIGURE_FILE(
    "config.h.in"
    "config.h"
)

CONFIGURE_FILE(ProcIO.cpp ProcIO.procmon.cpp)
CONFIGURE_FILE(ProcIO.cpp ProcIO.reducer.cpp)

SET( CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11" )

INCLUDE_DIRECTORIES ( "${CMAKE_CURRENT_SOURCE_DIR}" )
INCLUDE_DIRECTORIES ( "${CMAKE_CURRENT_BINARY_DIR}" )

## Discover dependencies
IF ( USE_BOOST )
    SET( Boost_USE_STATIC_LIBS ON )
    SET( Boost_USE_MULTITHREADED OFF )
    SET( Boost_USE_STATIC_RUNTIME ON )
    FIND_PACKAGE( Boost 1.54.0 COMPONENTS program_options )
    INCLUDE_DIRECTORIES ( "${Boost_INCLUDE_DIRS}" )
    SET ( REDUCER_LINK ${REDUCER_LINK} ${Boost_LIBRARIES} )
ENDIF ( USE_BOOST )

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
ADD_EXECUTABLE( ProcReducer ProcReducer.cpp ProcIO.reducer.cpp ProcReducerData.cc )
    SET_TARGET_PROPERTIES( ProcReducer PROPERTIES COMPILE_FLAGS -DUSE_CONFIG_H=1 )
ADD_EXECUTABLE( ProcMuxer ProcMuxer.cpp ProcIO.reducer.cpp )
    SET_TARGET_PROPERTIES( ProcMuxer PROPERTIES COMPILE_FLAGS -DUSE_CONFIG_H=1 )
ADD_EXECUTABLE( CheckH5 CheckH5.cpp ProcIO.reducer.cpp )
    SET_TARGET_PROPERTIES( CheckH5 PROPERTIES COMPILE_FLAGS -DUSE_CONFIG_H=1 )
ADD_EXECUTABLE( PostReducer PostReducer.cpp ProcIO.reducer.cpp ProcReducerData.cc )
    SET_TARGET_PROPERTIES( PostReducer PROPERTIES COMPILE_FLAGS -DUSE_CONFIG_H=1 )

IF ( USE_REDUCER_HDF5 )
    append_compile_flags_to_target( CheckH5 -DUSE_HDF5=1 )
    TARGET_LINK_LIBRARIES( CheckH5 ${HDF5_LIBRARIES} )
    append_compile_flags_to_target( ProcReducer -DUSE_HDF5=1 )
    TARGET_LINK_LIBRARIES( ProcReducer ${HDF5_LIBRARIES} )
    append_compile_flags_to_target( PostReducer -DUSE_HDF5=1 )
    TARGET_LINK_LIBRARIES( PostReducer ${HDF5_LIBRARIES} )
    append_compile_flags_to_target( ProcMuxer -DUSE_HDF5=1 )
    TARGET_LINK_LIBRARIES( ProcMuxer ${HDF5_LIBRARIES} )
ENDIF ( USE_REDUCER_HDF5 )
IF ( USE_REDUCER_AMQP )
    append_compile_flags_to_target( CheckH5 -DUSE_AMQP=1 )
    TARGET_LINK_LIBRARIES( CheckH5 ${RABBITMQ_C_LIBRARIES} )
    append_compile_flags_to_target( ProcReducer -DUSE_AMQP=1 )
    TARGET_LINK_LIBRARIES( ProcReducer ${RABBITMQ_C_LIBRARIES} )
    append_compile_flags_to_target( PostReducer -DUSE_AMQP=1 )
    TARGET_LINK_LIBRARIES( PostReducer ${RABBITMQ_C_LIBRARIES} )
    append_compile_flags_to_target( CheckH5 -DUSE_AMQP=1 )
    TARGET_LINK_LIBRARIES( CheckH5 ${RABBITMQ_C_LIBRARIES} )
ENDIF ( USE_REDUCER_AMQP )
