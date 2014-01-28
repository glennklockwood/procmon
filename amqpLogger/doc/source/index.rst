.. amqpLogger documentation master file, created by
   sphinx-quickstart on Thu Oct  3 08:09:44 2013.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Introduction
============

amqpLogger is a utility and utility library to ease scalable data collection.

Features
--------

* simple yet flexible logging capabilities
* built-in metadata context for logged messages
* transport of messages via RabbitMQ message exchange
* publication of messages to topic exchange to enable intelligent message
  selection and routing
* SWIG bindings enable perl, python, and TCL interfaces
* Optional node-level daemon service to reduce message transmission time
  (i.e., by allowing sensors/loggers to make a UNIX domain connection)

Contents
--------

.. toctree::
   :maxdepth: 2

   amqpLogger
