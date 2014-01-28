BUILT_SOURCES = $(srcdir)/amqpLogger_wrap.c
SWIG_SOURCES = $(top_srcdir)/src/amqpLogger.i

tclinst_LTLIBRARIES = amqpLogger.la
amqpLogger_la_SOURCES = amqpLogger_wrap.c $(SWIG_SOURCES) $(top_srcdir)/src/amqpLogger.c
amqpLogger_la_CPPFLAGS = -I$(top_srcdir)/src $(librabbitmq_CFLAGS) $(TCL_CPPFLAGS)
amqpLogger_la_LDFLAGS = -no-undefined -module -avoid-version $(librabbitmq_LINKOPTS) -lrt

$(srcdir)/amqpLogger_wrap.c: $(SWIG_SOURCES)
	$(SWIG) -tcl8 -I$(top_srcdir)/src -o $@ $<

install-data-hook:
	find $(tclinstdir) -name \*.a -delete
	find $(tclinstdir) -name \*.la -delete

CLEANFILES = $(BUILT_SOURCES)
