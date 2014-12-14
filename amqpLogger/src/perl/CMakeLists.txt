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
INCLUDE_DIRECTORIES(${PERL_INCLUDE_PATH})
INCLUDE_DIRECTORIES(${CMAKE_CURRENT_SOURCE_DIR})
INCLUDE_DIRECTORIES(${CMAKE_CURRENT_SOURCE_DIR}/..)

SWIG_ADD_MODULE( amqpLogger-perl perl ${SWIGSRC} )
#SWIG_LINK_LIBRARIES( amqpLogger-perl ${PERL_LIBRARY} ) ### DO NOT LINK IN THE PERL LIB!
SWIG_LINK_LIBRARIES( amqpLogger-perl ${AMQPLOGGER_LIBRARIES} )
    SET_TARGET_PROPERTIES( amqpLogger-perl PROPERTIES COMPILE_FLAGS "${PERL_EXTRA_C_FLAGS}")
    SET_TARGET_PROPERTIES( amqpLogger-perl PROPERTIES OUTPUT_NAME "amqpLogger" )
    SET_TARGET_PROPERTIES( amqpLogger-perl PROPERTIES PREFIX "" )



