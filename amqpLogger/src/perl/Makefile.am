BUILT_SOURCES = $(srcdir)/amqpLogger_wrap.c
SWIG_SOURCES = $(top_srcdir)/src/amqpLogger.i

perlinst_DATA = amqpLogger.pm
perlinst_LTLIBRARIES = amqpLogger.la
amqpLogger_la_SOURCES = amqpLogger_wrap.c $(SWIG_SOURCES) $(top_srcdir)/src/amqpLogger.c
amqpLogger_la_CPPFLAGS = -I$(top_srcdir)/src $(librabbitmq_CFLAGS) $(PERL_EXT_CPPFLAGS) -I$(PERL_EXT_INC)
amqpLogger_la_LDFLAGS = -no-undefined -module -avoid-version $(librabbitmq_LINKOPTS) -lrt

$(srcdir)/amqpLogger_wrap.c: $(SWIG_SOURCES)
	$(SWIG) -perl -I$(top_srcdir)/src -o $@ $<

install-data-hook:
	find $(perlinstdir) -name \*.a -delete
	find $(perlinstdir) -name \*.la -delete

CLEANFILES = $(BUILT_SOURCES) amqpLogger.pm
