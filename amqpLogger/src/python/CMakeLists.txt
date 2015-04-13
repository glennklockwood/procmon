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
INCLUDE_DIRECTORIES(${PYTHON_INCLUDE_DIRS})
INCLUDE_DIRECTORIES(${CMAKE_CURRENT_SOURCE_DIR})
INCLUDE_DIRECTORIES(${CMAKE_CURRENT_SOURCE_DIR}/..)
FIND_PROGRAM(PYTHON "python")

SET(PythonInstDir lib/python${PYTHON_VERSION_MAJOR}.${PYTHON_VERSION_MINOR}/site-packages)

SWIG_ADD_MODULE( amqpLogger python ${SWIGSRC} )
SWIG_LINK_LIBRARIES( amqpLogger ${PYTHON_LIBRARIES} )
SWIG_LINK_LIBRARIES( amqpLogger ${AMQPLOGGER_LIBRARIES} ${RABBITMQ_C_LIBRARIES} "rt" )

ADD_CUSTOM_COMMAND ( OUTPUT amqpLogger.pyc
        COMMAND python -m compileall .
        DEPENDS amqpLogger.py
)
ADD_CUSTOM_TARGET (amqpLoggerPython ALL DEPENDS amqpLogger.pyc )
INSTALL(
        FILES
            ${CMAKE_CURRENT_BINARY_DIR}/amqpLogger.py
            ${CMAKE_CURRENT_BINARY_DIR}/amqpLogger.pyc
            ${CMAKE_CURRENT_BINARY_DIR}/_amqpLogger.so
        DESTINATION ${PythonInstDir}
)

