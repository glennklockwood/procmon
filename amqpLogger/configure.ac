#                                               -*- Autoconf -*-
# Process this file with autoconf to produce a configure script.
AC_PREREQ([2.67])

AC_INIT(amqpLogger, 1.0, dmj@nersc.gov)
AC_PROG_LIBTOOL
AC_CONFIG_MACRO_DIR([m4])
AM_INIT_AUTOMAKE([amqpLogger], [1.0])

AC_ARG_ENABLE([staticBin], AS_HELP_STRING([--disable-static-bin], [Do not build static executables]))
AS_IF([test "x$enable_staticBin" != "xno"], [
    dnl do something
])

AC_CONFIG_SRCDIR([config.h.in])
AC_CONFIG_HEADERS([config.h])

AM_PATH_PYTHON(2.6)
AX_PKG_SWIG([2.0.0])
AX_SWIG_PYTHON

CMU_TCL

AC_PATH_PROG([PERL],[perl])
AX_PROG_PERL_VERSION([5.8.0])
AX_PERL_EXT

AC_DEFINE([AMQPLOGGER_NAME],["amqpLogger"], [release name])
AC_DEFINE([AMQPLOGGER_VERSION_MAJOR], [1], ["Major Version Number"])
AC_DEFINE([AMQPLOGGER_VERSION_MINOR], [0], ["Minor Version Number"])

AC_ARG_WITH([amqp_server], AS_HELP_STRING([--with-amqp-server], [AMQP Server Name]),
        [AC_DEFINE_UNQUOTED([AMQPLOGGER_SERVER], ["$with_amqp_server"], ["AMQP Server"])],
        [AC_MSG_ERROR([Must specify with-amqp-server])]
)
AC_ARG_WITH([amqp_port], AS_HELP_STRING([--with-amqp-port], [AMQP Port]),
        [],
        [with_amqp_port=5672]
)
AC_DEFINE_UNQUOTED([AMQPLOGGER_PORT], [$with_amqp_port], ["AMQP Port"])
AC_ARG_WITH([amqp_vhost], AS_HELP_STRING([--with-amqp-vhost], [AMQP Virtual Host]),
        [AC_DEFINE_UNQUOTED([AMQPLOGGER_VHOST], ["$with_amqp_vhost"], ["AMQP Virtual Host"])],
        [AC_MSG_ERROR([Must specify with-amqp-vhost])]
)
AC_ARG_WITH([amqp_user], AS_HELP_STRING([--with-amqp-user], [AMQP Username]),
        [AC_DEFINE_UNQUOTED([AMQPLOGGER_USER], ["$with_amqp_user"], ["AMQP Username"])],
        [AC_MSG_ERROR([Must specify with-amqp-user])]
)
AC_ARG_WITH([amqp_password], AS_HELP_STRING([--with-amqp-password], [AMQP Password]),
        [AC_DEFINE_UNQUOTED([AMQPLOGGER_PASSWORD], ["$with_amqp_password"], ["AMQP Password"])],
        [AC_MSG_ERROR([Must specify with-amqp-password])]
)
AC_ARG_WITH([amqp_exchange], AS_HELP_STRING([--with-amqp-exchange], [AMQP Exchange]),
        [AC_DEFINE_UNQUOTED([AMQPLOGGER_EXCHANGE], ["$with_amqp_exchange"], ["AMQP Exchange"])],
        [AC_MSG_ERROR([Must specify with-amqp-exchange])]
)
AC_ARG_WITH([amqp_framesize], AS_HELP_STRING([--with-amqp-framesize], [AMQP Framesize]),
        [],
        [with_amqp_framesize=131072]
)
AC_DEFINE_UNQUOTED([AMQPLOGGER_FRAMESIZE], [$with_amqp_framesize], ["AMQP Framesize"])
AC_ARG_WITH([amqp_retries], AS_HELP_STRING([--with-amqp-retries], [AMQP retries]),
        [],
        [with_amqp_retries=3]
)
AC_DEFINE_UNQUOTED([AMQPLOGGER_RETRIES], [$with_amqp_retries], ["AMQP retries"])

AC_DEFINE([DEFAULT_SYSTEM], "", ["default system marker"])
AC_DEFINE([DEFAULT_HOSTNAME], "", ["default hostname marker"])
AC_DEFINE([DEFAULT_IDENTIFIER], "", ["default identifier marker"])
AC_DEFINE([DEFAULT_SUBIDENTIFIER], "", ["default subidentifier marker"])

AC_ARG_WITH([socket_path], AS_HELP_STRING([--with-socket-path], [unix domain socket location]),
        [],
        [with_socket_path="/tmp/amqpLogger.sock"]
)
AC_DEFINE_UNQUOTED([SOCKET_PATH], ["$with_socket_path"], ["socket location"])
AC_ARG_WITH([socket_backlog], AS_HELP_STRING([--with-socket-backlog], [unix domain socket backlog]),
        [],
        [with_socket_backlog=50]
)
AC_DEFINE_UNQUOTED([SOCKET_BACKLOG], [$with_socket_backlog], ["backlog connections to allow"])


# Checks for programs.
AC_PROG_CC
AM_PROG_CC_C_O

# Checks for libraries.

# Checks for header files.
AC_CHECK_HEADERS([fcntl.h stdlib.h string.h sys/socket.h unistd.h])

# Checks for typedefs, structures, and compiler characteristics.
AC_TYPE_PID_T
AC_TYPE_SIZE_T
AC_TYPE_SSIZE_T

# Checks for library functions.
AC_FUNC_FORK
AC_CHECK_FUNCS([bzero gethostname localtime_r memset socket strchr])

PKG_CHECK_MODULES([librabbitmq], [librabbitmq], [
    AC_SUBST([librabbitmq_PREFIX], [$( pkg-config --variable=prefix librabbitmq )])
],)

AC_CHECK_FILE([$librabbitmq_PREFIX/lib/librabbitmq.fpic.a], [
    AC_SUBST([librabbitmq_LINKOPTS], [$librabbitmq_PREFIX/lib/librabbitmq.fpic.a])], [
    AC_SUBST([librabbitmq_LINKOPTS], [$librabbitmq_LIBS])])

AC_SUBST([tclinstdir], [$libdir/tcl/amqpLogger])
AC_SUBST([perlinstdir], [$libdir/perl/$PERL_VERSION/amqpLogger])

AC_CONFIG_FILES([Makefile src/Makefile src/python/Makefile src/tcl/Makefile src/perl/Makefile amqpLogger.pc])
AC_OUTPUT
