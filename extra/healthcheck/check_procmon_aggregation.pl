#!/usr/bin/perl
#####################################################################
##
## check_procmon_aggregation.pl
##
## Author: Douglas Jacobsen
## Date  : 2014/09/18
## Copyright (C) 2014 the Regents of the University of California
##
## This nagios plugin will check the health of procmon data collection
## and aggregation processes.
##
#####################################################################

use strict;
use warnings;

use constant NORMAL   => 0;
use constant WARNING  => 1;
use constant CRITICAL => 2;
use constant UNKNOWN  => 3;

my @statusMessages = ("OK", "Warning", "Critical", "Unknown");

## default configurations
my $buildableCriticalWait = 5400;
my $buildableWarningWait  = 3600;
my $buildableCountCritical = 10;
my $buildableCountWarning = 5;
my $startTime = time();

## import dependencies
use Getopt::Long;
use Pod::Usage;
use List::Util qw[min max];

sub parse_arguments {
    my $ret = {
        "procmonDir"    => undef,
        "user"          => undef,
        "verbose"       => 0,
        "timeout"       => 15,
    };

    my $help = 0;
    GetOptions("procmonDir=s" => \$ret->{procmonDir},
               "user|u=s"     => \$ret->{user},
               "timeout|t=i"  => \$ret->{timeout},
               "help|?|H"     => \$help,
    ) or pod2usage({
        -exitval => 2,
        -message => "Error in command line arguments"
    });
    if ($help) {
        pod2usage(1);
    }
    if (!defined($ret->{procmonDir}) || !defined($ret->{user})) {
        pod2usage({
            -exitval => 2,
            -message => "Error: procmonDir, user required!"
        });
    }
    return $ret;
}

sub readProcmonManagerConfig {
    my ($config) = @_;
    my $configFilePath = $config->{procmonDir} . "/etc/procmonManager.conf";
    if (-e $configFilePath) {
        open(my $fd, "<", $configFilePath) or return undef;
        while (<$fd>) {
            chomp($_);
            my $line = $_;
            if ($line =~ /=/) {
                my ($key, $value) = split(/=/, $line, 2);
                $key =~ s/^\s*//g;
                $key =~ s/\s*$//g;
                $value =~ s/\s*$//g;
                $value =~ s/^\s*//g;
                $config->{$key} = $value;
            }
        }
        close($fd);
        return $config;
    }
    return undef;
}

sub findProcmonManagerProcess {
    my ($options) = @_;
    my @pids = `pgrep -u $options->{user}`;
    foreach my $pid (@pids) {
        chomp($pid);
        if (-e "/proc/$pid/cmdline") {
            open(my $fd, "<", "/proc/$pid/cmdline") or next;
            while (<$fd>) {
                my @args = split(/\0/, $_);
                if ($#args >= 1) {
                    if ($args[0] =~ /python/ && $args[1] =~ /procmonManager.py/) {
                        return $pid;
                    }
                }
            }
        }
    }
    return undef;
}

sub findProcMuxerProcesses {
    my ($options) = @_;
    my @pids = `pgrep -u $options->{user}`;
    my @found = ();
    my @found_pids = ();
    foreach my $pid (@pids) {
        chomp($pid);
        if (-e "/proc/$pid/cmdline") {
            open(my $fd, "<", "/proc/$pid/cmdline") or next;
            while (<$fd>) {
                my @args = split(/\0/, $_);
                if ($args[0] =~ /ProcMuxer/) {
                    my $group = undef;
                    my $id = undef;
                    for (my $idx = 0; $idx <= $#args; $idx++) {
                        if ($args[$idx] == "-g" && $idx < $#args) {
                            $idx++;
                            $group = $args[$idx];
                        }
                        if ($args[$idx] == "-i" && $idx < $#args) {
                            $idx++;
                            $id = $args[$idx];
                        }
                    }
                    if (defined($id) && defined($group) && $group == $options->{group}) {
                        push(@found, $id);
                        push(@found_pids,  $pid);
                    }
                }
            }
        }
    }
}


my $options = parse_arguments();
my $status = 0;

my $nagiosStatus = NORMAL;
my @nagiosMessages = ();

if (!defined(readProcmonManagerConfig($options))) {
    $nagiosStatus = CRITICAL;
    push(@nagiosMessages, "FAILED to read procmonManger.conf");
    goto TheEnd; ## can't proceed, so just cleanup and die
}

my $pid = findProcmonManagerProcess($options);
if (!defined($pid) || $pid <= 0) {
    $nagiosStatus = CRITICAL;
    push(@nagiosMessages, "procmonManager.py process not running!");
}

findProcMuxerProcesses($options);



TheEnd:
if (time() - $startTime > $options->{timeout}) {
    $nagiosStatus = CRITICAL;
    push(@nagiosMessages, "Timeout: $options->{timeout}");
}
my $messages = join('|', @nagiosMessages);
if (length($messages) == 0) {
    $messages = "No queue problems detected";
}
print "$0 $statusMessages[$nagiosStatus]: $messages\n";
exit($nagiosStatus);
__END__

=head1 NAME

check_jenkins_queue.pl - Examine jenkins build queue and set off alarms

=head1 SYNOPSIS

check_jenkins_queue.pl [options]

 Options:
   -h|--host <hostname>            hostname of jenkins master webservice
   -r|--root <path/to/jenkins>     path on URL string to jenkins root
   -u|--user <username>            user account, should have limited access
   -t|--token <apiToken>           jenkins API token for <username>
   -w|--warning <integer>          max # of buildable queued items for warning
   -c|--critical <integer>         max # of buildable queued items for critical
   -W|--waitwarn <seconds>         max # of seconds for a buildable queued job
   -C|--waitcrit <seconds>         max # of seconds for a buildable queued job
   -v|--verbose                    flag for verbose output
   -x|--xml <xmlfile>              parse xml file instead of downloading
   -T|--timeout <seconds>          max time for completion before critical alert
   -h|-?|--help                    display this help

=head1 DESCRIPTION

check_jenkins_queue.pl attempts to connect to the configured jenkins master
server and count the number of jobs builable jobs queued to be run.  If the
webserver does not return HTTP code 200, or if the counted number of queued,
buildable items exceeds the critical or warning limits, then the appropriate
alert is issued.  In addition, if any jobs are marked as "stuck" or have been
waiting to build for configurable, alarmable timeouts, then the appropriate
alert is issued.

=head1 AUTHOR

Douglas Jacobsen <dmj@nersc.gov>
September 11, 2014

=cut
