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
                        if ($args[$idx] eq "-g" && $idx < $#args) {
                            $idx++;
                            $group = $args[$idx];
                        }
                        if ($args[$idx] eq "-i" && $idx < $#args) {
                            $idx++;
                            $id = $args[$idx];
                        }
                    }
                    if (defined($id) && defined($group) && $group eq $options->{group}) {
                        push(@found, $id);
                        push(@found_pids,  $pid);
                    }
                }
            }
        }
    }
    return (\@found, \@found_pids);
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

my @found = findProcMuxerProcesses($options);
my @found_ids = @{$found[0]};
my @found_pids = @{$found[1]};

if ($#found_ids != $#found_pids) {
    $nagiosStatus = UNKNOWN;
    push(@nagiosMessages, "Found inconsistent ProcMuxer processes, check manually!");
    goto TheEnd; ## can't proceed, so just cleanup and die
}
if ($#found_ids+1 != $options->{num_procmuxers}) {
    $nagiosStatus = WARNING;
    push(@nagiosMessages, "Incorrect number of ProcMuxers running in the group");
}
if ($#found_ids < 0) {
    $nagiosStatus = CRITICAL;
    push(@nagiosMessages, "No ProcMuxers running in the $options->{group} group");
}

my $localPath = "$options->{base_prefix}/$options->{group}";
my $archivingPath = "$localPath/archiving";
if ($options->{use_hpss} eq "True") {
    if (! -e $archivingPath) {
        $nagiosStatus = CRITICAL;
        push(@nagiosMessages, "Archiving enabled, but no archiving path; something is wrong!");
    } else {
        opendir(my $dh, $archivingPath);
        my @h5 = grep { /h5$/ && -f $archivingPath/$_ } readdir($dh);
        closedir($dh);
        if ($#h5 > 2) {
            my $nfiles = $#h5 + 1;
            push(@nagiosMessages, "Archiving queue building: $nfiles files waiting");
            if ($#h5 > 5) {
                $nagiosStatus = CRITICAL;
            } else {
                $nagiosStatus = WARNING;
            }
        }
    }
}
my $processingPath = "$localPath/processing";
if (! -e $processingPath) {
    $nagiosStatus = CRITICAL;
    push(@nagiosMessages, "No processing path exists; something is wrong!");
} else {
    opendir(my $dh, $processingPath);
    my $prefix = $options->{h5_prefix};
    my @files = readdir($dh);
    my @h5 = grep (/^$prefix/, @files);
    closedir($dh);
    if ($#h5 > 0) {
        my $nfiles = $#h5 + 1;
        push(@nagiosMessages, "Processing directory is filling up: $nfiles untransferred h5 files exist.");
        $nagiosStatus = CRITICAL;
    }
}


TheEnd:
if (time() - $startTime > $options->{timeout}) {
    $nagiosStatus = CRITICAL;
    push(@nagiosMessages, "Timeout: $options->{timeout}");
}
my $messages = join('|', @nagiosMessages);
if (length($messages) == 0) {
    $messages = "No procmon data aggregation problems detected";
}
print "$0 $statusMessages[$nagiosStatus]: $messages\n";
exit($nagiosStatus);
__END__

=head1 NAME

check_procmon_aggregation.pl - Examine procmon aggregation (procmonManager) state via NRPE and set off alarms

=head1 SYNOPSIS

check_procmon_aggregation.pl [options]

 Options:
   -h|--procmonDir <path>          path to procmon installation
   -u|--user <username>            user account running aggregation framework
   -v|--verbose                    flag for verbose output
   -h|-?|--help                    display this help

=head1 DESCRIPTION

check_procmon_aggregation.pl runs via NRPE on a data collection node, checks
on the state of the procMuxer processes, procmonManager.py process, and the
state of the local directories to determine if data are stacking up.

=head1 AUTHOR

Douglas Jacobsen <dmj@nersc.gov>
October 2, 2014

=cut
