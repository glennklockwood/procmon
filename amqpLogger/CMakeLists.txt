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

SET( AMQPLOGGER_NAME amqpLogger )
SET( AMQPLOGGER_MAJOR_VERSION 2 )
SET( AMQPLOGGER_MINOR_VERSION 5 )

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


ADD_SUBDIRECTORY( src )
ADD_SUBDIRECTORY( scripts )
ADD_SUBDIRECTORY( examples )
