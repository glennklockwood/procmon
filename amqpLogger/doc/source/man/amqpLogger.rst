:orphan:

amqpLogger Utility manual page
==============================

**amqpLogger** [*options*] <*message tag*> <*strings* ...>

Description
-----------

:program:`amqpLogger` detects metadata context according to installation configuration
file, or in a file specified by the environmental variable AMQPLOGGER_CONFIG.

Options
-------

.. option:: -h, --help
   
   Display help message
   
.. option:: -v, --verbose

   Verbose output (option currently unused)

.. option:: -V, --version

   Print amqpLogger version
   
.. option:: -d, --daemon

   Run amqpLogger multiplexer daemon

.. option:: -t, --delimiter <char>

   Use delimiter instead of ','

.. option:: -s, --system <string>

   Override SYSTEM tag (avoid this)

.. option:: -H, --host   <string>

   Override HOST tag (avoid this)

.. option:: -I, --identifier <string>

   Override IDENTIFIER tag, defaults to job id or INTERACTIVE if not in a job context.

.. option:: -S, --subidentifier <string>

   Override SUBIDENTIFIER tag, defaults to task ID or NA if not in a job context

.. option:: -p, --pidfile <path>

   Used a pid file to control and record daemon startup, placed in <path>

.. option:: -u, --user <string>

   If daemon started as root, drop privileges to named user.

.. option:: -g, --group <string>

   If daemon started as root, drop privileges to named group.


Messaging Mode
--------------

Running amqpLogger (without --daemon) will send a message to the configured
RabbitMQ instance, on the configured RabbitMQ exchange.  The message is
delivered to the topic exchange using publisher commits - meaning that all
efforts are made to ensure the message actually reaches the exchange.  **It is
the responsiblity of the user to ensure that a client is connected to receive
the message.**

Daemon Mode
-----------

When run with the --daemon option, amqpLogger will daemonize and listen for
local client (Messaging Mode) instances of amqpLogger and amqpLogger library
connections to expedite the routing of those messages.  
