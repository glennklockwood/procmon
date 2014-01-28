BUILT_SOURCES = $(srcdir)/amqpLogger_wrap.c
SWIG_SOURCES = $(top_srcdir)/src/amqpLogger.i

pkgpython_PYTHON = amqpLogger.py
pkgpyexec_LTLIBRARIES = _amqpLogger.la
_amqpLogger_la_SOURCES = amqpLogger_wrap.c $(SWIG_SOURCES) $(top_srcdir)/src/amqpLogger.c
_amqpLogger_la_CPPFLAGS = $(PYTHON_CPPFLAGS) -I$(top_srcdir)/src $(librabbitmq_CFLAGS)
_amqpLogger_la_LDFLAGS = -no-undefined -module -avoid-version $(librabbitmq_LINKOPTS) -lrt

$(srcdir)/amqpLogger_wrap.c: $(SWIG_SOURCES)
	$(SWIG) -python -I$(top_srcdir)/src -o $@ $<

install-data-hook:
	mv $(pkgpythondir)/amqpLogger.py $(pkgpythondir)/__init__.py
	mv $(pkgpythondir)/amqpLogger.pyc $(pkgpythondir)/__init__.pyc
	mv $(pkgpythondir)/amqpLogger.pyo $(pkgpythondir)/__init__.pyo

install-exec-hook:
	find $(pkgpyexecdir) -name \*.a -delete
	find $(pkgpyexecdir) -name \*.la -delete
CLEANFILES = $(BUILT_SOURCES) amqpLogger.py
