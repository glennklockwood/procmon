#@reboot root test if /usr/sbin/runprocmon && /etc/init.d/procmon start
#0 * * * * root /etc/init.d/procmon status >/dev/null || /etc/init.d/procmon start
