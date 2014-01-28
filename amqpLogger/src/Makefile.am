SUBDIRS = . python tcl perl
bin_PROGRAMS = amqpLogger
lib_LTLIBRARIES = libamqpLogger.la
include_HEADERS = amqpLogger.h

amqpLogger_SOURCES = main.c amqpLogger.c
amqpLogger_LDFLAGS = -static -lrt
amqpLogger_CFLAGS = $(librabbitmq_CFLAGS)
amqpLogger_LDADD = $(librabbitmq_LIBS)

libamqpLogger_la_SOURCES = amqpLogger.c
libamqpLogger_la_CFLAGS = $(librabbitmq_CFLAGS)
libamqpLogger_la_LIBADD = $(librabbitmq_LINKOPTS)
libamqpLogger_la_LDFLAGS = -version-info 1:0:0
