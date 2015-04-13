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

SET( SWIGSRC ../amqpLogger.i )
INCLUDE_DIRECTORIES(${TCL_INCLUDE_PATH})
INCLUDE_DIRECTORIES(${CMAKE_CURRENT_SOURCE_DIR})
INCLUDE_DIRECTORIES(${CMAKE_CURRENT_SOURCE_DIR}/..)


#  TCL_LIBRARY            = path to Tcl library (tcl tcl80)
#  TCL_INCLUDE_PATH       = path to where tcl.h can be found
SWIG_ADD_MODULE( amqpLogger-tcl tcl8 ${SWIGSRC} )
SWIG_LINK_LIBRARIES( amqpLogger-tcl ${AMQPLOGGER_LIBRARIES} ${RABBITMQ_C_LIBRARIES} "rt")
    SET_TARGET_PROPERTIES( amqpLogger-tcl PROPERTIES OUTPUT_NAME "amqpLogger" )
    SET_TARGET_PROPERTIES( amqpLogger-tcl PROPERTIES PREFIX "" )

INSTALL(
        FILES ${CMAKE_CURRENT_BINARY_DIR}/amqpLogger.so
        DESTINATION lib/tcl
)
