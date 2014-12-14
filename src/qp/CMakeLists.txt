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

INCLUDE_DIRECTORIES ( "${CMAKE_CURRENT_SOURCE_DIR}" )
INCLUDE_DIRECTORIES ( "${CMAKE_CURRENT_BINARY_DIR}" )
INCLUDE_DIRECTORIES ( "${CMAKE_CURRENT_SOURCE_DIR}/.." )
INCLUDE_DIRECTORIES ( "${CMAKE_CURRENT_BINARY_DIR}/.." )

## setup nclq
SET (FLEX_DIR $ENV{FLEX_DIR})
SET (NCLQ_DIR ${CMAKE_BINARY_DIR}/nclq)
ExternalProject_Add(
    nclq
    GIT_REPOSITORY git@bitbucket.org:berkeleylab/nersc-nclq.git
    CMAKE_ARGS "-DFLEX_INCLUDE_DIR=${FLEX_DIR}/include"
        "-DVALGRIND_INCLUDE_DIR=${VALGRIND_INCLUDE_DIR}"
        "-DCMAKE_INSTALL_PREFIX=${NCLQ_DIR}"
        "-DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}"
    TEST_BEFORE_INSTALL 1
)
ADD_EXECUTABLE( qp client.cpp QProc.cpp QProc.hh )
ADD_EXECUTABLE( qpd server.cpp QProc.cpp QProc.hh )

